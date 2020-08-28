package elasticsearch

import (
	"context"
	"errors"
	"github.com/limitedlee/microservice/common/config"
	"github.com/olivere/elastic/v7"
	"github.com/spf13/cast"
	"time"
)

type esClient struct {
	c         *elastic.Client
	ctx       context.Context
	boolQuery *elastic.BoolQuery
	query     elastic.Query
	querys    []elastic.Query
	orQuerys  []elastic.Query
	collapse  *elastic.CollapseBuilder
}

func NewEsClient() (*esClient, error) {
	host := config.GetString("elasticsearch")
	uname := config.GetString("elasticsearch.LoginName")
	passwd := config.GetString("elasticsearch.Password")

	//创建连接
	client, err := elastic.NewClient(
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
		elastic.SetURL(host),
		elastic.SetBasicAuth(uname, passwd),
	)
	if err != nil {
		return nil, newEsError(err)
	}

	return &esClient{
		c:   client,
		ctx: context.Background(),
	}, nil
}

//设置超时时间
func (client *esClient) SetTimeout(timeout time.Duration) {
	client.ctx, _ = context.WithTimeout(client.ctx, timeout)
}

//设置must查询条件（类似sql里的and）
func (client *esClient) SetQuery(name string, values []interface{}) {
	if len(values) == 0 { //不带条件查询，返回全部数据
		client.querys = append(client.querys, elastic.NewMatchAllQuery())
	} else if len(values) == 1 { //单字段单条件查询
		client.querys = append(client.querys, elastic.NewMatchQuery(name, values[0]))
	} else { //单字段多条件查询，类似sql里的in
		client.querys = append(client.querys, elastic.NewTermsQuery(name, values...))
	}
}

//设置should查询条件（类似sql里的or）
//args[0]：是否忽略TF/IDF,1是0否
//args[1]：权重值
func (client *esClient) SetOrQuery(name string, values interface{}, args ...interface{}) {
	if len(args) > 0 && args[0] == 1 {
		client.query = elastic.NewConstantScoreQuery(elastic.NewMatchQuery(name, values))
	} else {
		client.query = elastic.NewMatchQuery(name, values)
	}

	if len(args) > 1 {
		client.setBoost(cast.ToFloat64(args[1]))
	}

	client.orQuerys = append(client.orQuerys, client.query)
}

//设置查询权重
func (client *esClient) setBoost(boost float64) {
	switch client.query.(type) {
	case *elastic.MatchQuery:
		client.query.(*elastic.MatchQuery).Boost(boost)
	case *elastic.ConstantScoreQuery:
		client.query.(*elastic.ConstantScoreQuery).Boost(boost)
	}
}

//设置折叠条件
func (client *esClient) SetCollapse(innerHitName, field string, size int, sort []elastic.Sorter) {
	innerHit := elastic.NewInnerHit().Name(innerHitName).Size(size)
	if len(sort) > 0 {
		innerHit.SortBy(sort...)
	}

	client.collapse = elastic.NewCollapseBuilder(field).InnerHit(innerHit)
}

//设置排序条件
func (client *esClient) SetSortBy(field string, ascending bool) elastic.Sorter {
	sorter := elastic.NewFieldSort(field)
	if ascending {
		sorter = sorter.Asc()
	} else {
		sorter = sorter.Desc()
	}

	return sorter
}

//设置geo距离排序条件
//latLon 坐标经纬度，纬度在前
func (client *esClient) SetGeoDistanceSortBy(fieldName, latLon, unit string, ascending bool) elastic.Sorter {
	pointStr, _ := elastic.GeoPointFromString(latLon)
	return elastic.NewGeoDistanceSort(fieldName).Points(pointStr).Unit(unit).Order(ascending)
}

//搜索
func (client *esClient) Search(indexName string, pageIndex, pageSize int, sort []elastic.Sorter) (*elastic.SearchResult, error) {
	if pageIndex <= 0 {
		pageIndex = 1
	}
	if pageSize <= 0 {
		pageIndex = 10
	}

	client.boolQuery = elastic.NewBoolQuery()
	if len(client.querys) > 0 {
		client.boolQuery.Must(client.querys...)
	}
	if len(client.orQuerys) > 0 {
		client.boolQuery.Should(client.orQuerys...)
	}

	ss := client.c.Search().Index(indexName).Query(client.boolQuery)

	if client.collapse != nil {
		ss = ss.Collapse(client.collapse)
	}
	if len(sort) > 0 {
		for _, s := range sort {
			ss = ss.SortBy(s)
		}
	}

	res, err := ss.From(pageIndex - 1).Size(pageSize).Pretty(true).Do(client.ctx)
	if err != nil {
		return nil, newEsError(err)
	}

	return res, nil
}

//删除
func (client *esClient) Delete(indexName, id string) error {
	doc, err := client.c.Delete().
		Index(indexName).
		Id(id).
		Refresh("wait_for").
		Do(client.ctx)
	if err != nil {
		return newEsError(err)
	}
	if doc.Result != "deleted" {
		return newEsError(errors.New("删除失败"))
	}

	return nil
}

//批量操作
func (client *esClient) Bulk(data []elastic.BulkableRequest) error {
	if len(data) == 0 {
		return errors.New("no bulk data")
	}

	bulkRequest := client.c.Bulk()

	for _, v := range data {
		bulkRequest = bulkRequest.Add(v)
	}

	_, err := bulkRequest.
		Refresh("wait_for").
		Do(client.ctx)
	if err != nil {
		return newEsError(err)
	}

	//if bulkRequest.NumberOfActions() == 0 {
	//	glog.Infof("Actions all clear!")
	//}

	return nil
}

//保存
func (client *esClient) Save(indexName, id string, data interface{}) error {
	_, err := client.c.Index().
		Index(indexName).
		Id(id).
		BodyJson(data).
		Refresh("wait_for").
		Do(client.ctx)
	if err != nil {
		return newEsError(err)
	}

	return nil
}
