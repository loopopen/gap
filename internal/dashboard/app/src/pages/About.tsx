import React from 'react';
import { Badge, List, Spin, Typography } from 'antd';
import { useMetas } from '../hooks/useMetas';
import { GithubFilled } from '@ant-design/icons';
import { Link } from 'react-router-dom';

const { Paragraph } = Typography;

const About: React.FC = () => {
  const { metas, loading } = useMetas();

  return (
    <div>
      <h1>About GAP Dashboard</h1>
      <Paragraph>This is the GAP Dashboard application built with React and Ant Design.</Paragraph>
      <Paragraph>Created by Lopolop Inc.</Paragraph>
      <h1>About GAP<Link to="https://github.com/lopolopen/gap" target="_blank" style={{ marginLeft: 8 }}><GithubFilled /></Link></h1>
      <div style={{ maxWidth: '60%' }}>
        <Paragraph>GAP is a lightweight, event-driven messaging library for Go. </Paragraph>
        <Paragraph>It provides outbox pattern implementation with support for RabbitMQ, Kafka and MySQL (or GORM-based storage).</Paragraph>
        <Paragraph>It is designed to support additional brokers and databases in the future.</Paragraph>
      </div>
      <Spin spinning={loading}>
        <List
          dataSource={metas}
          locale={{ emptyText: 'No metas found' }}
          renderItem={(item) => (
            <List.Item>
              {
                item.type === 'Self' ?
                  <List.Item.Meta
                    title={<Badge key={'gap'} color={'blue'} text={'GAP'} />}
                    description={`version: ${item.version}`}
                  />
                  :
                  <List.Item.Meta
                    title={<Badge key={item.plugin} color={'green'} text={`${item.type} / ${item.plugin}`} />}
                    description={`version: ${item.version}`}
                  />
              }
            </List.Item>
          )}
        />
      </Spin>
    </div>
  );
};

export default About;