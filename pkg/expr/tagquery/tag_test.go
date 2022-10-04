package tagquery

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseTags(t *testing.T) {
	type args struct {
		tags []string
	}
	tests := []struct {
		name    string
		args    args
		want    Tags
		wantErr bool
	}{
		{
			name: "three simple tags",
			args: args{
				tags: []string{"aaa=aaa", "bbb=bbb", "ccc=ccc"},
			},
			want: Tags{
				{Key: "aaa", Value: "aaa"},
				{Key: "bbb", Value: "bbb"},
				{Key: "ccc", Value: "ccc"},
			},
		}, {
			name: "three tags with one invalid one",
			args: args{
				tags: []string{"aaa=aaa", "bbb=bbb", "ccc="},
			},
			wantErr: true,
		}, {
			name: "empty list of args",
			want: Tags{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTags(tt.args.tags)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseTags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseTagsFromMetricName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		args    args
		want    Tags
		wantErr bool
	}{
		{
			name: "simple metric name with tags",
			args: args{
				name: "my.test.metric;tag1=value1;tag2=value2",
			},
			want: Tags{
				{Key: "name", Value: "my.test.metric"},
				{Key: "tag1", Value: "value1"},
				{Key: "tag2", Value: "value2"},
			},
		}, {
			name: "simple metric name without tags",
			args: args{
				name: "my.test.metric",
			},
			want: Tags{
				{Key: "name", Value: "my.test.metric"},
			},
		}, {
			name: "metric name with ~",
			args: args{
				name: "~my~~~~.te~st.met~ric~;aaa=bbb",
			},
			want: Tags{
				{Key: "aaa", Value: "bbb"},
				{Key: "name", Value: "my~~~~.te~st.met~ric~"},
			},
		}, {
			name: "invalid tag in name1",
			args: args{
				name: "my.test.metric;aaa=",
			},
			wantErr: true,
		}, {
			name: "invalid tag in name2",
			args: args{
				name: "my.test.metric;aaa=;",
			},
			wantErr: true,
		}, {
			name: "invalid tag in name3",
			args: args{
				name: "my.test.metric;=bbb",
			},
			wantErr: true,
		}, {
			name: "invalid tag in name4",
			args: args{
				name: "my.test.metric;aaa=bbb;",
			},
			wantErr: true,
		}, {
			name: "invalid tag in name5",
			args: args{
				name: "my.test.metric;",
			},
			wantErr: true,
		}, {
			name: "invalid tag in name6",
			args: args{
				name: "my.test.metric;a=~b",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTagsFromMetricName(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTagsFromMetricName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseTagsFromMetricName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTags_Strings(t *testing.T) {
	tests := []struct {
		name string
		t    Tags
		want []string
	}{
		{
			name: "simple tag",
			t: Tags{
				{
					Key:   "aaa",
					Value: "bbb",
				},
			},
			want: []string{"aaa=bbb"},
		},
		{
			name: "multiple tags unsorted",
			t: Tags{
				{
					Key:   "ccc",
					Value: "xxx",
				}, {
					Key:   "bbb",
					Value: "yyy",
				}, {
					Key:   "aaa",
					Value: "zzz",
				},
			},
			want: []string{"ccc=xxx", "bbb=yyy", "aaa=zzz"},
		},
		{
			name: "empty arg list",
			t:    Tags{},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.Strings(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tags.Strings() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseTag(t *testing.T) {
	type args struct {
		tag string
	}
	tests := []struct {
		name    string
		args    args
		want    Tag
		wantErr bool
	}{
		{
			name: "simple tag",
			args: args{tag: "aaa=bbb"},
			want: Tag{Key: "aaa", Value: "bbb"},
		}, {
			name:    "empty string",
			args:    args{tag: ""},
			wantErr: true,
		}, {
			name:    "invalid key",
			args:    args{tag: "aa^a=bbb"},
			wantErr: true,
		}, {
			name: "invalid value",
			args: args{tag: "aaa=bb~b"},
			want: Tag{Key: "aaa", Value: "bb~b"},
		}, {
			name:    "invalid value",
			args:    args{tag: "aaa=~bbb"},
			wantErr: true,
		}, {
			name: "weird chars in tag1",
			args: args{tag: "a~a=bbb"},
			want: Tag{Key: "a~a", Value: "bbb"},
		}, {
			name: "weird chars in tag2",
			args: args{tag: "~=bbb"},
			want: Tag{Key: "~", Value: "bbb"},
		}, {
			name: "weird chars in tag3",
			args: args{tag: "~@#$%&*()=bbb"},
			want: Tag{Key: "~@#$%&*()", Value: "bbb"},
		}, {
			name: "weird chars in value1",
			args: args{tag: "aaa=b!b"},
			want: Tag{Key: "aaa", Value: "b!b"},
		}, {
			name: "weird chars in value2",
			args: args{tag: "aaa=!"},
			want: Tag{Key: "aaa", Value: "!"},
		}, {
			name: "weird chars in value3",
			args: args{tag: "aaa=!@#$%^&*()"},
			want: Tag{Key: "aaa", Value: "!@#$%^&*()"},
		}, {
			name:    "missing key",
			args:    args{tag: "=bbb"},
			wantErr: true,
		}, {
			name:    "missing value",
			args:    args{tag: "aaa="},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTag(tt.args.tag)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseTag() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTag_StringIntoBuilder(t *testing.T) {
	type fields struct {
		Key   string
		Value string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "simple tag",
			fields: fields{
				Key:   "aaa",
				Value: "bbb",
			},
			want: "aaa=bbb",
		}, {
			name:   "empty values",
			fields: fields{},
			want:   "=",
		}, {
			name: "weird chars",
			fields: fields{
				Key:   "~!@#$%",
				Value: "^&*()_+",
			},
			want: "~!@#$%=^&*()_+",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tag := &Tag{
				Key:   tt.fields.Key,
				Value: tt.fields.Value,
			}
			builder := &strings.Builder{}
			tag.StringIntoWriter(builder)
			got := builder.String()
			if got != tt.want {
				t.Errorf("StringIntoWriter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTagSorting(t *testing.T) {
	tags1 := Tags{Tag{Key: "key1", Value: "value1"}, Tag{Key: "key2", Value: "value2"}}
	tags2 := Tags{Tag{Key: "key2", Value: "value2"}, Tag{Key: "key1", Value: "value1"}}

	if reflect.DeepEqual(tags1, tags2) {
		t.Fatalf("Expected tags1 and tags2 to be different")
	}

	tags1.Sort()
	tags2.Sort()

	if !reflect.DeepEqual(tags1, tags2) {
		t.Fatalf("Expected tags1 and tags2 to be the same")
	}
}
