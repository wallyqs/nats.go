package vnext

type Msg interface {
}

type MsgHandler interface {
	ProcessMsg(Msg)
}

type MsgHandlerFunc func(Msg)

func (fn MsgHandlerFunc) ProcessMsg(msg Msg) {
	fn(msg)
}

type Subscription interface {
}

type Client interface {
	Publish(subj string, data []byte)
	Subscribe(subj string, cb MsgHandler) (Subscription, error)
}
