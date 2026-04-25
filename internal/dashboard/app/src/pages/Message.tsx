import React, { useState, useEffect } from "react";
import type { GetProp, TablePaginationConfig, TableProps } from 'antd';
import { Badge, Divider, message, Popover, Table, Tag } from 'antd';
import { usePubMsgs, useRecMsgs } from "../hooks/useMsgs";
import type { MsgQueryParams, PubMsgQueryParams, RecMsgQueryParams } from '../hooks/useMsgs';
import { MsgStatus, MsgStatusColors, type Msg } from "../types";
import { CopyOutlined, MailOutlined, UnorderedListOutlined } from "@ant-design/icons";
import JsonView from 'react18-json-view';
import 'react18-json-view/src/style.css';
import { Flex, Radio } from 'antd';
import type { SorterResult } from "antd/es/table/interface";

type ColumnsType<T extends object> = TableProps<T>['columns'];
type TablePagination = TablePaginationConfig;

interface TableParams {
  pagination?: TablePagination;
  sortField?: SorterResult<any>['field'];
  sortOrder?: SorterResult<any>['order'];
  filters?: Parameters<GetProp<TableProps, 'onChange'>>[1];
}

const pubColumns: ColumnsType<Msg> = [
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
    render: (_, r) => <Badge color={MsgStatusColors[r.status]} text={r.status} />,
    filters: [
      MsgStatus.Succeeded,
      MsgStatus.Processing,
      MsgStatus.Pending,
      MsgStatus.Failed,
    ].map(s => ({
      text: <Badge color={MsgStatusColors[s]} text={s} />,
      value: s,
    })),
    filterMultiple: false,
  },
  {
    title: 'Headers / Payload',
    render: (_, r) => (
      <span>
        <Popover content={<JsonView src={JSON.parse(r.headers)} />} title={'Message Headers'}>
          <a><UnorderedListOutlined /> </a>
        </Popover>
        <Divider style={{ color: 'black' }} vertical />
        <Popover content={<JsonView src={JSON.parse(r.payload)} />} title={'Message Payload'}>
          <a style={{ marginRight: 8 }}><MailOutlined /></a>
        </Popover>
        {r.payload}
      </span>)
  },
  {
    title: 'Topic',
    dataIndex: 'topic',
  }
];

const recColumns: ColumnsType<Msg> = [...pubColumns || [],
{
  title: 'Group',
  dataIndex: 'group',
}
]

function toQueryParams(params: TableParams): PubMsgQueryParams | RecMsgQueryParams {
  const query: MsgQueryParams = {
    page: params.pagination?.current,
    per_page: params.pagination?.pageSize,
    status: params.filters?.status?.[0] as MsgStatus | undefined,
    topic: params.filters?.topic?.[0] as string | undefined,
  };

  if (params.filters?.group?.[0]) {
    return {
      ...query,
      group: params.filters.group[0] as string | undefined,
    };
  }

  return query;
}

const Message: React.FC = () => {
  const [tableParams, setTableParams] = useState<TableParams>({
    pagination: {
      current: 1,
      pageSize: 10,
    },
  });

  const params = toQueryParams(tableParams);

  console.log(params.toString())

  const [msgType, setMsgType] = useState<'pub' | 'rec'>('pub')

  const pub = usePubMsgs(params as PubMsgQueryParams, {
    enabled: msgType === 'pub',
  });

  const rec = useRecMsgs(params as RecMsgQueryParams, {
    enabled: msgType === 'rec',
  });

  const { msgs, total, loading } = msgType === 'pub' ? pub : rec;

  const columns = msgType === 'pub' ? pubColumns : recColumns;

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
    filters: Record<string, (React.Key | boolean)[] | null>,
    // sorter: any
  ) => {
    setTableParams({
      filters,
      // sortOrder: Array.isArray(sorter) ? undefined : sorter.order,
      // sortField: Array.isArray(sorter) ? undefined : sorter.field,
      pagination: {
        ...pagination,
        total: tableParams.pagination?.total || 0,
      },
    });
  };

  return (
    <div>
      <div style={{ width: '240px', marginBottom: '12px' }}>
        <Flex vertical gap="medium">
          <Radio.Group defaultValue={msgType} buttonStyle="solid" onChange={(e) => {
            setMsgType(e.target.value);
            setTableParams({
              filters: {},
              pagination: {
                ...tableParams.pagination!,
                current: 1,
              },
            });
          }}>
            <Radio.Button value="pub">Published</Radio.Button>
            <Radio.Button value="rec">Received</Radio.Button>
          </Radio.Group>
        </Flex>
      </div>
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
    </div>
  );
}

export default Message;
