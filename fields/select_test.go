package fields_test

import (
	"errors"
	"reflect"
	"testing"

	"mongo-elastic-sync/fields"
)

func TestSelect(t *testing.T) {
	type args struct {
		doc      map[string]interface{}
		mappings []fields.M
	}
	tests := []struct {
		name string
		args args
		want map[string]interface{}
		err  error
	}{
		{
			name: "select single field",
			args: args{
				doc:      map[string]interface{}{"field1": "hello", "field2": "world"},
				mappings: []fields.M{{"field2"}},
			},
			want: map[string]interface{}{"field2": "world"},
		},
		{
			name: "select missing field",
			args: args{
				doc:      map[string]interface{}{"field1": "hello"},
				mappings: []fields.M{{"field5"}},
			},
			want: map[string]interface{}{},
		},
		{
			name: "select all with empty mappings",
			args: args{
				doc:      map[string]interface{}{"field1": "hello", "field2": "world"},
				mappings: []fields.M{},
			},
			want: map[string]interface{}{"field1": "hello", "field2": "world"},
		},
		{
			name: "select nested field",
			args: args{
				doc:      map[string]interface{}{"field1": "hello", "field2": map[string]interface{}{"nested1": "foo", "nested2": "bar"}},
				mappings: []fields.M{{"field2.nested1"}},
			},
			want: map[string]interface{}{"field2": map[string]interface{}{"nested1": "foo"}},
		},
		{
			name: "select missing nested field",
			args: args{
				doc:      map[string]interface{}{"field2": map[string]interface{}{"nested1": "foo", "nested2": "bar"}},
				mappings: []fields.M{{"field2.nested6"}},
			},
			want: map[string]interface{}{"field2": map[string]interface{}{}},
		},
		{
			name: "select all in nested field",
			args: args{
				doc:      map[string]interface{}{"field1": "hello", "field2": map[string]interface{}{"nested1": "foo", "nested2": "bar"}},
				mappings: []fields.M{{"field2"}},
			},
			want: map[string]interface{}{"field2": map[string]interface{}{"nested1": "foo", "nested2": "bar"}},
		},
		{
			name: "select multiple in nested field",
			args: args{
				doc:      map[string]interface{}{"field1": "hello", "field2": map[string]interface{}{"nested1": "foo", "nested2": "bar", "nested3": "make"}},
				mappings: []fields.M{{"field2.nested1"}, {"field2.nested3"}},
			},
			want: map[string]interface{}{"field2": map[string]interface{}{"nested1": "foo", "nested3": "make"}},
		},
		{
			name: "select unknown nested field",
			args: args{
				doc:      map[string]interface{}{"field1": "hello"},
				mappings: []fields.M{{"field1.nested1"}},
			},
			want: nil,
			err:  errors.New("unable to index field [field1.nested1], field [field1] is not a map"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fields.Select(tt.args.doc, tt.args.mappings)
			if !reflect.DeepEqual(err, tt.err) {
				t.Errorf("applyFieldMappings() error = %v, want %v", err, tt.err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("applyFieldMappings() got = %v, want %v", got, tt.want)
			}
		})
	}
}
