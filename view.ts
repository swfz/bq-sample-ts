import {BigQuery} from '@google-cloud/bigquery'

const exec = async () => {
  const bigquery = new BigQuery();
  const dataset = await bigquery.dataset('sample');
  const project = process.env['PROJECT']

  const query = `
SELECT
  d.dt,
  d.metadata_id,
  d.uk1,
  d.value1 as d_v1,
  d.value2,
  m.column1,
  m.value1 as m_v1
FROM
  \`${project}.sample.daily_data\` d
LEFT JOIN
  \`${project}.sample.metadata\` m
ON
  (m.metadata_id = d.metadata_id
    AND m.uk1 = d.uk1)
WHERE
  d.dt >= '2020-11-01';
`;

  const options = {
    view: query
  };

  const [view] = await dataset.createTable('sample_view', options)

  console.log(`view ${view.id} created.`)
}

exec();
