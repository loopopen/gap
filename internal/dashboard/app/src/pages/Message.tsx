import React, { useState } from 'react';
import type { GetProp, TablePaginationConfig, TableProps } from 'antd';
import { Badge, message, Popover, Radio, Table, Tag } from 'antd';
import { CopyOutlined, MailOutlined, UnorderedListOutlined } from '@ant-design/icons';
import type { SorterResult } from 'antd/es/table/interface';
import JsonView from 'react18-json-view';
import 'react18-json-view/src/style.css';
import { usePubMsgs, useRecMsgs } from '../hooks/useMsgs';
import type { MsgQueryParams, PubMsgQueryParams, RecMsgQueryParams } from '../hooks/useMsgs';
import { MsgStatus, MsgStatusColors, type Msg } from '../types';

type ColumnsType<T extends object> = TableProps<T>['columns'];
type TablePagination = TablePaginationConfig;

interface TableParams {
  pagination?: TablePagination;
  sortField?: SorterResult<Msg>['field'];
  sortOrder?: SorterResult<Msg>['order'];
  filters?: Parameters<GetProp<TableProps, 'onChange'>>[1];
}

const parseJson = (value: string) => {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
};

const pubColumns: ColumnsType<Msg> = [
  {
    title: 'ID',
    dataIndex: 'id',
    width: 235,
    render: (id: string) => (
      <span className="copy-id">
        <button
          type="button"
          aria-label={`Copy message ID ${id}`}
          onClick={() => {
            navigator.clipboard.writeText(id);
            message.info(`ID: ${id} copied.`);
          }}
        >
          <CopyOutlined />
        </button>
        {id}
      </span>
    ),
  },
  {
    title: 'Version',
    dataIndex: 'version',
    width: 100,
    render: (version: string) => <Tag className="version-tag">{version}</Tag>,
  },
  {
    title: 'Created',
    dataIndex: 'createdAt',
    width: 170,
  },
  {
    title: 'Status',
    dataIndex: 'status',
    width: 150,
    render: (_, record) => (
      <span className="status-cell">
        <Badge color={MsgStatusColors[record.status]} text={record.status} />
      </span>
    ),
    filters: [
      MsgStatus.Succeeded,
      MsgStatus.Processing,
      MsgStatus.Pending,
      MsgStatus.Failed,
    ].map((status) => ({
      text: <Badge color={MsgStatusColors[status]} text={status} />,
      value: status,
    })),
    filterMultiple: false,
  },
  {
    title: 'Headers / Payload',
    width: 430,
    render: (_, record) => (
      <span className="payload-cell">
        <Popover content={<JsonView src={parseJson(record.headers)} />} title="Message headers">
          <button className="payload-action" type="button" aria-label="View message headers">
            <UnorderedListOutlined />
          </button>
        </Popover>
        <Popover content={<JsonView src={parseJson(record.payload)} />} title="Message payload">
          <button className="payload-action" type="button" aria-label="View message payload">
            <MailOutlined />
          </button>
        </Popover>
        <span className="payload-preview" title={record.payload}>{record.payload}</span>
      </span>
    ),
  },
  {
    title: 'Topic',
    dataIndex: 'topic',
    width: 210,
  },
];

const recColumns: ColumnsType<Msg> = [
  ...(pubColumns ?? []),
  {
    title: 'Group',
    dataIndex: 'group',
    width: 180,
  },
];

function toQueryParams(params: TableParams): PubMsgQueryParams | RecMsgQueryParams {
  const query: MsgQueryParams = {
    page: params.pagination?.current,
    per_page: params.pagination?.pageSize,
    status: params.filters?.status?.[0] as MsgStatus | undefined,
    topic: params.filters?.topic?.[0] as string | undefined,
  };

  if (params.filters?.group?.[0]) {
    return { ...query, group: params.filters.group[0] as string };
  }

  return query;
}

const Message: React.FC = () => {
  const [tableParams, setTableParams] = useState<TableParams>({
    pagination: { current: 1, pageSize: 10 },
  });
  const [msgType, setMsgType] = useState<'pub' | 'rec'>('pub');
  const params = toQueryParams(tableParams);

  const pub = usePubMsgs(params as PubMsgQueryParams, { enabled: msgType === 'pub' });
  const rec = useRecMsgs(params as RecMsgQueryParams, { enabled: msgType === 'rec' });
  const { msgs, total, loading } = msgType === 'pub' ? pub : rec;
  const columns = msgType === 'pub' ? pubColumns : recColumns;

  const handleTableChange = (
    pagination: TablePaginationConfig,
    filters: Record<string, (React.Key | boolean)[] | null>,
  ) => {
    setTableParams({
      filters,
      pagination: { ...pagination, total },
    });
  };

  return (
    <div>
      <header className="page-heading">
        <div>
          <span className="page-eyebrow">Message observability</span>
          <h1>Messages</h1>
          <p>Inspect delivery state, routing details, headers, and payloads across GAP.</p>
        </div>
      </header>

      <section className="content-card">
        <div className="card-header">
          <div>
            <h2>{msgType === 'pub' ? 'Published messages' : 'Received messages'}</h2>
            <p>{total ? `${total} messages found` : 'Message delivery records'}</p>
          </div>
          <Radio.Group
            className="message-switch"
            value={msgType}
            buttonStyle="solid"
            onChange={(event) => {
              setMsgType(event.target.value);
              setTableParams({
                filters: {},
                pagination: { ...tableParams.pagination!, current: 1 },
              });
            }}
          >
            <Radio.Button value="pub">Published</Radio.Button>
            <Radio.Button value="rec">Received</Radio.Button>
          </Radio.Group>
        </div>

        <Table<Msg>
          className="message-table"
          columns={columns}
          rowKey={(record) => record.id}
          dataSource={msgs}
          pagination={{ ...tableParams.pagination, total, showSizeChanger: true }}
          loading={loading}
          onChange={handleTableChange}
          scroll={{ x: 1260 }}
        />
      </section>
    </div>
  );
};

export default Message;
