package openapi

import "strings"

func (m *MessageDataTypes) UnmarshalText(b []byte) {
	str := strings.Trim(string(b), `"`)

	switch {
	case str == "Embedded":
		*m = MESSAGEDATATYPES_EMBEDDED
	case str == "FileReference":
		*m = MESSAGEDATATYPES_FILE_REFERENCE
	default:
		*m = MESSAGEDATATYPES_EMBEDDED
	}

}
