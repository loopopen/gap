import React, { useState, useEffect } from "react";
import type { TablePaginationConfig, TableProps } from 'antd';
import { Badge, message, Table, Tag } from 'antd';
import { usePubMsgs } from "../hooks/useMsgs";
import { MsgStatusColors, type Msg } from "../types";
import { CopyOutlined } from "@ant-design/icons";

type ColumnsType<T extends object> = TableProps<T>['columns'];
type TablePagination = TablePaginationConfig;

interface TableParams {
  pagination?: TablePagination;
}

const columns: ColumnsType<Msg> = [
  {
    title: 'ID',
    dataIndex: 'id',
    render: (id) => (
      <span>
        <a style={{ marginRight: 8 }}>
          <CopyOutlined
            onClick={() => {
              navigator.clipboard.writeText(id);
              message.info(`ID: ${id} Copied.`)
            }}
          />
        </a>
        {id}
      </span>
    )
  },
  {
    title: 'Version',
    dataIndex: 'version',
    render: (version) => <Tag color={'green'}>{version}</Tag>
  },
  {
    title: 'CreatedAt',
    dataIndex: 'createdAt',
  },
  {
    title: 'Status',
    dataIndex: 'status',
    render: (_, r) => <Badge color={MsgStatusColors[r.status]} text={r.status} />
  },
  {
    title: 'Headers',
    dataIndex: 'headers',
  },
  {
    title: 'Payload',
    dataIndex: 'payload',
  },
  {
    title: 'Topic',
    dataIndex: 'topic',
  }
];

const Message: React.FC = () => {
  const [tableParams, setTableParams] = useState<TableParams>({
    pagination: {
      current: 1,
      pageSize: 10,
    },
  });

  const { msgs, total, loading } = usePubMsgs({
    page: tableParams.pagination?.current || 1,
    per_page: tableParams.pagination?.pageSize || 10,
  });

  useEffect(() => {
    setTableParams(prev => ({
      ...prev,
      pagination: {
        ...prev.pagination!,
        total: total || 0,
      },
    }));
  }, [total]);

  const handleTableChange = (
    pagination: TablePaginationConfig,
    // filters: Record<string, (React.Key | boolean)[] | null>,
    // sorter: any
  ) => {
    setTableParams({
      pagination: {
        ...pagination,
        total: tableParams.pagination?.total || 0,
      },
    });
  };

  return (
    <div>
      <Table<Msg>
        columns={columns}
        rowKey={(record) => record.id}
        dataSource={msgs}
        pagination={{
          ...tableParams.pagination,
          showSizeChanger: true,
        }}
        loading={loading}
        onChange={handleTableChange}
      />
    </div>
  );
}

export default Message;