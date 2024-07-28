package utils

func ClearChannel(ch any) {
	if ch, ok := ch.(chan any); ok {
		for {
			select {
			case <-ch:
			default:
				return
			}
		}
	}
}
