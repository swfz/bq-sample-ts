import {BigQuery} from '@google-cloud/bigquery'

const exec = async () => {
  const bigquery = new BigQuery();

  const rows = [
    {dt: '2020-11-01', metadata_id: 1, uk1: 'hoge', value1: 2, value2: 2},
    {dt: '2020-11-02', metadata_id: 1, uk1: 'hoge', value1: 2, value2: 2},
    {dt: '2020-11-03', metadata_id: 1, uk1: 'hoge', value1: 2, value2: 2},
    {dt: '2020-11-04', metadata_id: 1, uk1: 'hoge', value1: 2, value2: 2},
    {dt: '2020-11-05', metadata_id: 1, uk1: 'hoge', value1: 2, value2: 2},
    {dt: '2020-11-01', metadata_id: 1, uk1: 'fuga', value1: 2, value2: 2},
    {dt: '2020-11-02', metadata_id: 1, uk1: 'fuga', value1: 2, value2: 2},
    {dt: '2020-11-03', metadata_id: 1, uk1: 'fuga', value1: 2, value2: 2},
    {dt: '2020-11-04', metadata_id: 1, uk1: 'fuga', value1: 2, value2: 2},
    {dt: '2020-11-05', metadata_id: 1, uk1: 'fuga', value1: 2, value2: 2},
    {dt: '2020-11-01', metadata_id: 2, uk1: 'hoge', value1: 2, value2: 2},
    {dt: '2020-11-02', metadata_id: 2, uk1: 'hoge', value1: 2, value2: 2},
    {dt: '2020-11-03', metadata_id: 2, uk1: 'hoge', value1: 2, value2: 2},
    {dt: '2020-11-04', metadata_id: 2, uk1: 'hoge', value1: 2, value2: 2},
    {dt: '2020-11-05', metadata_id: 2, uk1: 'hoge', value1: 2, value2: 2},
    {dt: '2020-11-01', metadata_id: 2, uk1: 'fuga', value1: 2, value2: 2},
    {dt: '2020-11-02', metadata_id: 2, uk1: 'fuga', value1: 2, value2: 2},
    {dt: '2020-11-03', metadata_id: 2, uk1: 'fuga', value1: 2, value2: 2},
    {dt: '2020-11-04', metadata_id: 2, uk1: 'fuga', value1: 2, value2: 2},
    {dt: '2020-11-05', metadata_id: 2, uk1: 'fuga', value1: 2, value2: 2},
  ];
  const metadata = [
    {metadata_id: 1, uk1: 'hoge', column1: 'foo', value1: 1},
    {metadata_id: 2, uk1: 'hoge', column1: 'bar', value1: 1},
    {metadata_id: 1, uk1: 'fuga', column1: 'baz', value1: 1},
    {metadata_id: 2, uk1: 'fuga', column1: 'pon', value1: 1},
  ]
  await bigquery.dataset('sample').table('daily_data').insert(rows);
  await bigquery.dataset('sample').table('metadata').insert(metadata);
}

exec();