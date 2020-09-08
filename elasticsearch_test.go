package elasticsearch

import (
	"github.com/olivere/elastic/v7"
	"reflect"
	"testing"
)

type product struct {
	ProductName string `json:"product_name"` //商品名称
	ProductId   int32  `json:"product_id"`   //商品id
}

var indexName = "channel_product"

func TestSave(t *testing.T) {
	type args struct {
		index string
		id    string
		data  interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "T1",
			args: args{
				index: indexName,
				id:    "CX0013_1000068",
				data: product{
					ProductId:   1000068,
					ProductName: "阿闻爱佳前置仓商品01",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es, err := NewEsClient()
			if err != nil {
				t.Fatal(err)
			}

			if err := es.Save(tt.args.index, tt.args.id, tt.args.data); err != nil {
				t.Error(err)
				return
			}
		})
	}

	t.Log("save ok")
}

func TestSearch(t *testing.T) {
	type args struct {
		index     string
		pageIndex int
		pageSize  int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "T3",
			args: args{
				index:     indexName,
				pageIndex: 1,
				pageSize:  1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es, err := NewEsClient()
			if err != nil {
				t.Fatal(err)
			}

			es.SetQuery("", []interface{}{})
			res, err := es.Search(tt.args.index, tt.args.pageIndex, tt.args.pageSize, []elastic.Sorter{})
			if err != nil {
				t.Error(err)
				return
			}

			var typ product
			for _, item := range res.Each(reflect.TypeOf(typ)) { //从搜索结果中取数据的方法
				t.Logf("%#v\n", item.(product))
			}
		})
	}

	t.Log("search ok")
}

func TestDelete(t *testing.T) {
	type args struct {
		index string
		id    string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "T1",
			args: args{
				index: indexName,
				id:    "1000000",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es, err := NewEsClient()
			if err != nil {
				t.Fatal(err)
			}

			err = es.Delete(tt.args.index, tt.args.id)
			if err != nil {
				t.Error(err)
				return
			}
		})
	}

	t.Log("delete ok")
}

func TestBulk(t *testing.T) {
	type args struct {
		data []elastic.BulkableRequest
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "T1",
			args: args{
				data: []elastic.BulkableRequest{
					elastic.NewBulkIndexRequest().Index(indexName).Id("YC0127_1000083"),
					////修改
					//elastic.NewBulkIndexRequest().Index(indexName).Id("RP0009_1000005").Doc(product{
					//	ProductId:   1000005,
					//	ProductName: "东瓜1",
					//}),
					////新建
					//elastic.NewBulkIndexRequest().Index(indexName).Id("CX0013_1000002").Doc(product{
					//	ProductId:   1000012,
					//	ProductName: "测试商品C010101005",
					//}),
					////删除
					//elastic.NewBulkDeleteRequest().Index(indexName).Id("1000000"),
					////修改
					//elastic.NewBulkUpdateRequest().Index(indexName).Id("1000001").Doc(product{
					//	ProductId:   "1000001",
					//	ProductName: "西瓜2",
					//	FinanceCode: "CX0013",
					//}),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es, err := NewEsClient()
			if err != nil {
				t.Fatal(err)
			}

			res, err := es.Bulk(tt.args.data)
			if err != nil {
				t.Fatal(err)
				return
			}

			for _, v := range res.Items {
				t.Logf("%#v", v["index"])
			}
		})
	}

	t.Log("bulk ok")
}

func Test_esClient_MgetById(t *testing.T) {
	type args struct {
		m map[string][]string
	}
	tests := []struct {
		name    string
		args    args
		want    *elastic.MgetResponse
		wantErr bool
	}{
		{
			name: "通过id批量查询",
			args: args{
				m: map[string][]string{
					"channel_product": {"CX0004_1019418", "CX0004_10194181"},
				},
			},
			want:    &elastic.MgetResponse{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es, err := NewEsClient()
			if err != nil {
				t.Fatal(err)
			}

			got, err := es.MgetById(tt.args.m)
			if (err != nil) != tt.wantErr {
				t.Errorf("MgetById() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MgetById() got = %v, want %v", got, tt.want)
			}
		})
	}
}
