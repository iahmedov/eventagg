package eventagg

type Event struct {
	Type   string                 `json:"event_type"`
	Time   int64                  `json:"ts"`
	Params map[string]interface{} `json:"params"`
}
