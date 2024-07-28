package utils

func ClearChannel(ch any) chan any {
	if ch, ok := ch.(chan any); ok {
		for {
			select {
			case <-ch:
			default:
				return ch
			}
		}
	}

	return nil
}
