package vnext

type Subscription interface {
	Subject() string
	Queue() string
	Unsubscribe() error
	Drain() error
	// Pending
}

// Client is the next gen interface type for a NATS connection.
type Client interface {
	Publish(subj string, data []byte) error
	PublishRequest(subj, reply string, data []byte) error
	PublishMsg(Msg) error
	Subscribe(subj string, cb Handler) (Subscription, error)
	// QueueSubscribe(subj, queue string, cb MsgHandler) (Subscription, error)
	Drain() error
	Close()
	// Stats()  -> Statistics
	// Status() -> connected, closed, etc...
	// Info()   -> ServerID, ClientID, etc...
}

// type Handler interface {}

type Handler interface {
	ProcessMsg(Msg)
}

type MsgHandler func(Msg)

func (fn MsgHandler) ProcessMsg(msg Msg) {
	fn(msg)
}

type Msg interface {
	Subject() string
	Reply() string
	Header() Header
	Respond([]byte) error
	RespondMsg(Msg) error
}

type Header interface {
	Add(key, value string)
	Set(key, value string)
	Get(key string)
	Del(key string)
	Values(key string) []string
}
