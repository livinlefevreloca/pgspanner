package protocol

func MakeConnectionErrorMessages(
	message string,
	detail string,
	code string,
	routine string,
) *ErrorResponsePgMessage {
	return BuildErrorResponsePgMessage(
		map[string]string{
			"Localized Severity":    "Error",
			"Nonlocalized Severity": "ERROR",
			"Message":               message,
			"Detail":                detail,
			"Code":                  code,
			"Hint":                  "Check the pgspanner server logs for more information",
			"Routine":               routine,
		},
	)
}
