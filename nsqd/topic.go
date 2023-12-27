package nsqd

import (
	"errors"
	"hash/crc64"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mlib.com/nsq/internal/diskqueue"
	"mlib.com/nsq/internal/lg"
	"mlib.com/nsq/internal/quantile"
	"mlib.com/nsq/internal/util"
)

var (
	crc64ISOTable *crc64.Table = crc64.MakeTable(crc64.ISO)
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	MessageCount uint64 `json:"message_count"`
	MessageBytes uint64 `json:"message_bytes"`

	sync.RWMutex

	Name              string                `json:"name"`
	Channels          map[string]bool       `json:"channels"`
	backend           BackendQueue          `json:"-"`
	memoryMsgChan     chan *Message         `json:"-"`
	startChan         chan int              `json:"-"`
	exitChan          chan int              `json:"-"`
	channelUpdateChan chan int              `json:"-"`
	waitGroup         util.WaitGroupWrapper `json:"-"`
	ExitFlag          int32                 `json:"exit_flag"`
	idFactory         *guidFactory          `json:"-"`

	Ephemeral      bool         `json:"ephemeral"`
	deleteCallback func(*Topic) `json:"-"`
	deleter        sync.Once    `json:"-"`

	Paused    int32    `json:"paused"`
	pauseChan chan int `json:"-"`

	nsqd *NSQD `json:"-"`
}

// Topic constructor
func NewTopic(topicName string, nsqd *NSQD, deleteCallback func(*Topic)) *Topic {
	t := &Topic{
		Name:              topicName,
		Channels:          make(map[string]bool, 0),
		memoryMsgChan:     nil,
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		nsqd:              nsqd,
		Paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    deleteCallback,
		idFactory:         NewGUIDFactory(int64(crc64.Checksum([]byte(nsqd.getOpts().ID), crc64ISOTable))),
	}
	// create mem-queue only if size > 0 (do not use unbuffered chan)
	if nsqd.getOpts().MemQueueSize > 0 {
		t.memoryMsgChan = make(chan *Message, nsqd.getOpts().MemQueueSize)
	}
	if strings.HasSuffix(topicName, "#ephemeral") {
		t.Ephemeral = true
		t.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		t.backend = diskqueue.New(
			topicName,
			nsqd.getOpts().DataPath,
			nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			nsqd.getOpts().SyncEvery,
			nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	t.waitGroup.Wrap(t.messagePump)

	t.nsqd.Notify(t, !t.Ephemeral)

	return t
}

func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.ExitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	c, ok := t.nsqd.cluster.channels.Load(t.Name + ":" + channelName)
	if !ok {
		err := t.nsqd.cluster.NewChannel(t.Name, channelName)
		if err != nil {
			t.nsqd.logf(LOG_ERROR, "NewChannel failed:%v", err)
			return nil, true
		}
		c, ok = t.nsqd.cluster.channels.Load(t.Name + ":" + channelName)
		if !ok || c == nil {
			t.nsqd.logf(LOG_ERROR, "NewChannel failed:not saved in channels")
			return nil, true
		}
		t.Channels[channelName] = true
		t.nsqd.cluster.TopicUpdate(t.Name, "Channels", t.Channels)
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.Name, c.(*Channel).Name)
		return c.(*Channel), true
	}
	return c.(*Channel), false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.nsqd.cluster.channels.Load(t.Name + ":" + channelName)
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel.(*Channel), nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.Name, channelName)
	err := t.nsqd.cluster.DeleteChannel(t.Name, channelName)
	if err != nil {
		t.nsqd.logf(LOG_ERROR, "DeleteChannel failed: %v", err)
		return errors.New("DeleteChannel failed")
	}

	numChannels := len(t.Channels)
	t.Unlock()

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	if numChannels == 0 && t.Ephemeral {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.ExitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&t.MessageCount, 1)
	t.nsqd.cluster.TopicUpdate(t.Name, "MessageCount", t.MessageCount)

	atomic.AddUint64(&t.MessageBytes, uint64(len(m.Body)))
	t.nsqd.cluster.TopicUpdate(t.Name, "MessageBytes", t.MessageBytes)
	return nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.ExitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotalBytes := 0

	for i, m := range msgs {
		err := t.put(m)
		if err != nil {
			atomic.AddUint64(&t.MessageCount, uint64(i))
			t.nsqd.cluster.TopicUpdate(t.Name, "MessageCount", t.MessageCount)
			atomic.AddUint64(&t.MessageBytes, uint64(messageTotalBytes))
			t.nsqd.cluster.TopicUpdate(t.Name, "MessageBytes", t.MessageBytes)
			return err
		}
		messageTotalBytes += len(m.Body)
	}

	atomic.AddUint64(&t.MessageBytes, uint64(messageTotalBytes))
	t.nsqd.cluster.TopicUpdate(t.Name, "MessageBytes", t.MessageBytes)
	atomic.AddUint64(&t.MessageCount, uint64(len(msgs)))
	t.nsqd.cluster.TopicUpdate(t.Name, "MessageCount", t.MessageCount)
	return nil
}

func (t *Topic) put(m *Message) error {
	select {
	case t.memoryMsgChan <- m:
	default:
		err := writeMessageToBackend(m, t.backend)
		t.nsqd.SetHealth(err)
		if err != nil {
			t.nsqd.logf(LOG_ERROR,
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.Name, err)
			return err
		}
	}
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan <-chan []byte

	// do not pass messages before Start(), but avoid blocking Pause() or GetChannel()
	for {
		select {
		case <-t.channelUpdateChan:
			continue
		case <-t.pauseChan:
			continue
		case <-t.exitChan:
			goto exit
		case <-t.startChan:
		}
		break
	}
	t.RLock()
	for cn := range t.Channels {
		c, ok := t.nsqd.cluster.channels.Load(t.Name + ":" + cn)
		if ok && c != nil {
			chans = append(chans, c.(*Channel))
		}
	}
	t.RUnlock()
	if len(chans) > 0 && !t.IsPaused() {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	// main message loop
	for {
		select {
		case msg = <-memoryMsgChan:
		case buf = <-backendChan:
			msg, err = decodeMessage(buf)
			if err != nil {
				t.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		case <-t.channelUpdateChan:
			chans = chans[:0]
			t.RLock()
			for cn := range t.Channels {
				c, ok := t.nsqd.cluster.channels.Load(t.Name + ":" + cn)
				if ok && c != nil {
					chans = append(chans, c.(*Channel))
				}
			}
			t.RUnlock()
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.pauseChan:
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			goto exit
		}

		for i, channel := range chans {
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			if chanMsg.deferred != 0 {
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
				continue
			}
			err := channel.PutMessage(chanMsg)
			if err != nil {
				t.nsqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.Name, msg.ID, channel.Name, err)
			}
		}
	}

exit:
	t.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ... messagePump", t.Name)
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.ExitFlag, 0, 1) {
		return errors.New("exiting")
	}
	t.nsqd.cluster.TopicUpdate(t.Name, "ExitFlag", t.ExitFlag)

	if deleted {
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.Name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.nsqd.Notify(t, !t.Ephemeral)
	} else {
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.Name)
	}

	close(t.exitChan)

	// synchronize the close of messagePump()
	t.waitGroup.Wait()

	if deleted {
		t.Lock()
		for cn := range t.Channels {
			t.nsqd.cluster.DeleteChannel(t.Name, cn)
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	t.RLock()
	for cn := range t.Channels {
		c, ok := t.nsqd.cluster.channels.Load(t.Name + ":" + cn)
		if ok && c != nil {
			err := c.(*Channel).Close()
			if err != nil {
				// we need to continue regardless of error to close all the channels
				t.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", c.(*Channel).Name, err)
			}
		}
	}
	t.RUnlock()

	// write anything leftover to disk
	t.flush()
	return t.backend.Close()
}

func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}

func (t *Topic) flush() error {
	if len(t.memoryMsgChan) > 0 {
		t.nsqd.logf(LOG_INFO,
			"TOPIC(%s): flushing %d memory messages to backend",
			t.Name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(msg, t.backend)
			if err != nil {
				t.nsqd.logf(LOG_ERROR,
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.Channels))
	for cn := range t.Channels {
		c, ok := t.nsqd.cluster.channels.Load(t.Name + ":" + cn)
		if ok && c != nil {
			realChannels = append(realChannels, c.(*Channel))
		}
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.Paused, 1)
	} else {
		atomic.StoreInt32(&t.Paused, 0)
	}
	t.nsqd.cluster.TopicUpdate(t.Name, "Paused", t.Paused)

	select {
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.Paused) == 1
}

func (t *Topic) GenerateID() MessageID {
	var i int64 = 0
	for {
		id, err := t.idFactory.NewGUID()
		if err == nil {
			return id.Hex()
		}
		if i%10000 == 0 {
			t.nsqd.logf(LOG_ERROR, "TOPIC(%s): failed to create guid - %s", t.Name, err)
		}
		time.Sleep(time.Millisecond)
		i++
	}
}
