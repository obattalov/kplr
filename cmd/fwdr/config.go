package config

type (
	Config struct {
		AgregatorIP string
		RecieverIP string
		JournalName string
		KeyName string
	}

	Unit struct {
		Records []string
		LastNumber uint64
	}
)