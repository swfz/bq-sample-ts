import {BigQuery} from '@google-cloud/bigquery'

const exec = async () => {
  const bigquery = new BigQuery();

  const project = process.env['PROJECT']
  const dataset = 'sample'
  const tableId = 'metadata'
  const tmpTableId = 'metadata_tmp'

  const query = `
    MERGE \`${project}.${dataset}.${tableId}\` target USING \`${project}.${dataset}.${tmpTableId}\` tmp
    ON(target.metadata_id = tmp.metadata_id AND target.uk1 = tmp.uk1)
    WHEN MATCHED THEN
      UPDATE SET metadata_id = tmp.metadata_id, uk1 = tmp.uk1, column1 = tmp.column1, value1 = tmp.value1
    WHEN NOT MATCHED THEN
      INSERT(metadata_id, uk1, column1, value1)
      VALUES(metadata_id, uk1, column1, value1)
`;
  // tmpテーブルの作成
  const schema = [
    {name: 'metadata_id', type: 'INTEGER', mode: 'REQUIRED'},
    {name: 'uk1', type: 'STRING', mode: 'REQUIRED'},
    {name: 'column1', type: 'STRING',},
    {name: 'value1', type: 'INTEGER'},
  ]
  const tmpOptions = {
    schema: schema,
  }
  const [table] = await bigquery.dataset(dataset).createTable(tmpTableId, tmpOptions);
  console.log(`Table ${table.id} created.`);

  const metadata = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    writeDisposition: 'WRITE_TRUNCATE'
  }
  await bigquery.dataset(dataset).table(tmpTableId).load('./updated_metadata.json', metadata)
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