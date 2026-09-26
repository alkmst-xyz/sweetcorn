export type TableName = string
export type TableConfig = Record<
  TableName,
  {
    name: string
    columns: string[]
  }
>
