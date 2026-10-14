import React from 'react';
import { GithubOutlined, NodeIndexOutlined } from '@ant-design/icons';
import { List, Spin } from 'antd';
import { useMetas } from '../hooks/useMetas';

const About: React.FC = () => {
  const { metas, loading } = useMetas();

  return (
    <div>
      <header className="page-heading">
        <div>
          <span className="page-eyebrow">System information</span>
          <h1>About GAP</h1>
          <p>Runtime details for the dashboard and the modules connected to this application.</p>
        </div>
      </header>

      <div className="about-grid">
        <section className="content-card about-card">
          <h2>Event delivery for Go</h2>
          <p>
            GAP is a lightweight, event-driven messaging library for Go with an outbox-pattern
            implementation for reliable delivery.
          </p>
          <p>
            It supports RabbitMQ and Kafka brokers together with MySQL and GORM-based storage,
            while remaining open to additional integrations.
          </p>
          <a
            className="about-link"
            href="https://github.com/loopopen/gap"
            target="_blank"
            rel="noreferrer"
          >
            <GithubOutlined /> View source on GitHub
          </a>
        </section>

        <section className="content-card about-card">
          <h2>Loaded modules</h2>
          <Spin spinning={loading}>
            <List
              className="meta-list"
              dataSource={metas}
              locale={{ emptyText: 'No modules found' }}
              renderItem={(item) => (
                <List.Item>
                  <div className="meta-row">
                    <span className="meta-name">
                      <span className="meta-icon"><NodeIndexOutlined /></span>
                      {item.type === 'Self' ? 'GAP Core' : `${item.type} / ${item.plugin}`}
                    </span>
                    <span className="meta-version">{item.version}</span>
                  </div>
                </List.Item>
              )}
            />
          </Spin>
        </section>
      </div>
    </div>
  );
};

export default About;
