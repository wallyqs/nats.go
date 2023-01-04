package vnext

type Subscription interface {
	// Subject() string
	// Queue() string
	Unsubscribe() error
	Drain() error
	// Pending
}

// Conn is the next gen interface type for a NATS connection.
type Conn interface {
	Publish(subj string, data []byte) error
	PublishRequest(subj, reply string, data []byte) error
	// PublishMsg(Msg) error
	Subscribe(subj string, cb Handler) (Subscription, error)
	// QueueSubscribe(subj, queue string, cb Handler) (Subscription, error)
	Drain() error
	// Request
	// Flush() error
	Close()
	// Stats()  -> Statistics
	// Status() -> connected, closed, etc...
	// Info()   -> ServerID, ClientID, etc...
	// private()
}

type Handler interface {
	ProcessMsg(Msg)
}

// MsgHandler is a helper type to be able to create callbacks
// a la http.HandlerFunc.
type MsgHandler func(Msg)

func (fn MsgHandler) ProcessMsg(msg Msg) {
	fn(msg)
}

// Msg has to be a concrete type maybe?
// Nah...
type Msg interface {
	Subject() string
	Reply() string
	Data() []byte
	Header() Header
	// SetHeader(Header)
	Respond([]byte) error
	// RespondMsg(Msg) error
}

type Header interface {
	Add(key, value string)
	Set(key, value string)
	Get(key string) string
	Del(key string)
	Values(key string) []string
}
