package rabbitmq


type publisher struct {
	reliable bool
	url  string
	topic string
}

func (p *publisher) Publisher() error {
	channel, err := p.Channel()
	if err != nil {
		return err
	}
	defer func() {
		if err := channel.Close(); err != nil {
			fmt.Printf("")
		}
	}()
	return nil
}