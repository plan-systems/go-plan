

package pdi



// GetPartWithName returns the first-appearing BodyPart with the matching part and codec name
func (body *Body) GetPartWithName(inCodec string, inPartName string) *BodyPart {
    for _, part := range body.Parts {
        if part.Name == inPartName && part.ContentCodec == inCodec {
            return part
        }
    }
    return nil
}

// GetPartContentWithName returns the content of the first-appearing BodyPart with the matching part and codec name
func (body *Body) GetPartContentWithName(inCodec string, inPartName string) []byte {
    for _, part := range body.Parts {
        if part.Name == inPartName && part.ContentCodec == inCodec {
            return part.Content
        }
    }
    return nil
}

// GetPartContent returns the content of the first-appearing BodyPart with the matching codec name
func (body *Body) GetPartContent(inCodec string) []byte {
    for _, part := range body.Parts {
        if part.ContentCodec == inCodec {
            return part.Content
        }
    }
    return nil
}



