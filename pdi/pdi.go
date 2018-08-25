

package pdi




const (

    // ContentCodecHeaderName is the standard header field name used in PDIBodyPart.Headers used to describe PDIBodyPart.Content.
    // For info and background: https://github.com/multiformats/multicodec
    ContentCodecHeaderName = "Multicodec"
)

// GetHeaderValue searches for a header with a matching field name and returns its value
func (part *BodyPart) GetHeaderValue(inFieldName string) string {
    for _, entry := range part.Headers {
        if entry.FieldName == inFieldName {
            return entry.FieldValue 
        }
    }
    return ""
}
