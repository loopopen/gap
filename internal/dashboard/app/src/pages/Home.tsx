import React from 'react';
import {
  ArrowRightOutlined,
  DatabaseOutlined,
  GithubOutlined,
  MailOutlined,
  NodeIndexOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useMetas } from '../hooks/useMetas';

const Home: React.FC = () => {
  const { metas } = useMetas();
  const brokerCount = metas.filter((meta) => meta.type.toLowerCase().includes('broker')).length;
  const storageCount = metas.filter((meta) => meta.type.toLowerCase().includes('storage')).length;

  return (
    <div>
      <section className="home-hero">
        <div className="hero-copy">
          <span className="page-eyebrow">Event-driven messaging</span>
          <h1>See what is moving through GAP.</h1>
          <p>
            Inspect published and received messages, follow delivery status, and verify the
            modules powering this application from one operational view.
          </p>
          <div className="hero-actions">
            <Link className="primary-action" to="/message">
              Explore messages <ArrowRightOutlined />
            </Link>
            <a
              className="secondary-action"
              href="https://github.com/loopopen/gap"
              target="_blank"
              rel="noreferrer"
            >
              <GithubOutlined /> GitHub
            </a>
          </div>
        </div>

        <div className="hero-panel">
          <div>
            <span className="hero-panel-label">Runtime status</span>
            <strong>Healthy</strong>
          </div>
          <p>
            The dashboard is connected and ready to inspect message delivery across configured
            brokers and storage providers.
          </p>
        </div>
      </section>

      <section className="stat-grid" aria-label="GAP runtime overview">
        <div className="stat-card">
          <span className="stat-card-icon"><NodeIndexOutlined /></span>
          <span>Loaded modules</span>
          <strong>{metas.length || '—'}</strong>
        </div>
        <div className="stat-card">
          <span className="stat-card-icon"><MailOutlined /></span>
          <span>Broker plugins</span>
          <strong>{brokerCount || '—'}</strong>
        </div>
        <div className="stat-card">
          <span className="stat-card-icon"><DatabaseOutlined /></span>
          <span>Storage plugins</span>
          <strong>{storageCount || '—'}</strong>
        </div>
      </section>
    </div>
  );
};

export default Home;
