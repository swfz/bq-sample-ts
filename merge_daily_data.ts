import {BigQuery} from '@google-cloud/bigquery'

const exec = async () => {
  const bigquery = new BigQuery();

  const project = process.env['PROJECT']
  const dataset = 'sample'
  const tableId = 'daily_data'
  const tmpTableId = 'daily_data_tmp'

  const query = `
    MERGE \`${project}.${dataset}.${tableId}\` target USING \`${project}.${dataset}.${tmpTableId}\` tmp
    ON(target.dt = tmp.dt AND target.metadata_id = tmp.metadata_id AND target.uk1 = tmp.uk1 AND target.dt >= '2020-11-01')
    WHEN MATCHED THEN
      UPDATE SET dt = tmp.dt, metadata_id = tmp.metadata_id, uk1 = tmp.uk1, value1 = tmp.value1, value2 = tmp.value2
    WHEN NOT MATCHED THEN
      INSERT(dt, metadata_id, uk1, value1, value2)
      VALUES(dt, metadata_id, uk1, value1, value2)
`;
  // tmpテーブルの作成
  const schema = [
    {name: 'dt', type: 'DATE', mode: 'REQUIRED'},
    {name: 'metadata_id', type: 'INTEGER', mode: 'REQUIRED'},
    {name: 'uk1', type: 'STRING', mode: 'REQUIRED'},
    {name: 'value1', type: 'INTEGER',},
    {name: 'value2', type: 'INTEGER'},
  ]
  const tmpOptions = {
    schema: schema,
  }
  const [table] = await bigquery.dataset(dataset).createTable(tmpTableId, tmpOptions);
  console.log(`Table ${table.id} created.`);

  // 更新のロード
  const metadata = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    writeDisposition: 'WRITE_TRUNCATE'
  }
  await bigquery.dataset(dataset).table(tmpTableId).load('./updated_daily_data.json', metadata)
  const options = {
    query: query
  }
  const [job] = await bigquery.createQueryJob(options);

  console.log(`Job ${job.id} started.`);

  const [rows] = await job.getQueryResults();

  console.log('Rows:');
  rows.forEach(row => console.log(row))

  await bigquery.dataset(dataset).table(tmpTableId).delete();
}

exec();