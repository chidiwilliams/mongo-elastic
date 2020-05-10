package fields

import (
	"fmt"
	"strings"
)

// M represents a field mapping.
type M struct {
	Name string `yaml:"name"`
}

// Select returns a new doc transformed with the given fields mappings.
// If mappings is empty, all the fields in doc are returned. Else, only the fields in mappings are returned.
// Nested fields may be accessed using dot syntax (e.g. foo.bar.hello).
// If the nested field cannot be accessed (e.g. mapping foo.bar.hello, where bar is a boolean), an error is returned.
func Select(doc map[string]interface{}, mappings []M) (map[string]interface{}, error) {
	if len(mappings) == 0 {
		return doc, nil
	}

	// include only given fields
	newDoc := make(map[string]interface{}, len(doc))
	for _, mapping := range mappings {
		splitFields := strings.Split(mapping.Name, ".")

		// Traverse document and save value at specified path to newDoc
		var lastDocVal interface{} = doc    // value of last traversed field of doc
		var lastNewDocFieldMapPtr = &newDoc // ptr to (root or nested) map in newDoc to save to after traversing
		for i, field := range splitFields {
			lastDocValAsMap, ok := lastDocVal.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("unable to index field [%s], field [%s] is not a map", mapping.Name, strings.Join(splitFields[:i], "."))
			}

			// At most nested field, save value at field to lastNewDocFieldMapPtr if it exists.
			if i == len(splitFields)-1 {
				if finalValue, ok := lastDocValAsMap[field]; ok {
					(*lastNewDocFieldMapPtr)[field] = finalValue
				}
				break
			}

			// This is not the most nested field.
			// Update last traversed map lastNewDocFieldMapPtr with the map at key field.

			// If the value in the field already exists, use it as the new map.
			// This happens if a more nested value has already been copied to newDoc.
			// The value is definitely also a map, because lastDocVal from the original document is a map.
			// If the value in the field does not exist, create a map at that field and then use it as the new map.
			if v, ok := (*lastNewDocFieldMapPtr)[field]; ok {
				vAsMap := v.(map[string]interface{})
				lastNewDocFieldMapPtr = &vAsMap
			} else {
				newV := make(map[string]interface{})
				(*lastNewDocFieldMapPtr)[field] = newV
				lastNewDocFieldMapPtr = &newV
			}

			lastDocVal = lastDocValAsMap[field]
		}
	}

	return newDoc, nil
}
