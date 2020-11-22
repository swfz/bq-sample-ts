# bigquery node sample

- あまり変更のないデータとデイリーの数値データをユニークキーでjoinするサンプル
- unique key: metadata_id, uk1

## テーブルデータ作成

別途gcloudで

## insert-sample-data.ts
- サンプルデータ生成君

## merge_daily_data.ts
- daily_dataをMREGEでupsertする

## merge_metadata.ts
- metadataをMERGEでupsertする

## view.ts
- viewを作成する

## e.g.)

```
yarn ts-node-transpile-only merge_daily_data.ts
```
