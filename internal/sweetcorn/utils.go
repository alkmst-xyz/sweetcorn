package sweetcorn

import (
	"encoding/json"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func AttributesToBytes(attributes pcommon.Map) []byte {
	result := make(map[string]any)

	for k, v := range attributes.All() {
		result[k] = v.AsString()
	}

	b, _ := json.Marshal(result)
	return b
}

func AttributesArrayToBytes(attributesArray []pcommon.Map) []byte {
	result := make(map[string]any)

	for _, item := range attributesArray {
		for k, v := range item.All() {
			result[k] = v.AsString()
		}

	}

	b, _ := json.Marshal(result)
	return b
}
