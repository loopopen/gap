import React, { useMemo } from 'react';
import { ConfigProvider, Layout, Menu, Tooltip, theme } from 'antd';
import {
  GithubOutlined,
  HomeOutlined,
  InfoCircleOutlined,
  MailOutlined,
} from '@ant-design/icons';
import { BrowserRouter, Link, Route, Routes, useLocation } from 'react-router-dom';
import './App.css';
import { routes } from './config/routes';

const { Header, Content, Sider } = Layout;

const routeIcons: Record<string, React.ReactNode> = {
  '/': <HomeOutlined />,
  '/message': <MailOutlined />,
  '/about': <InfoCircleOutlined />,
};

const AppContent: React.FC = () => {
  const location = useLocation();
  const currentRoute = useMemo(
    () => routes.find((route) => route.path === location.pathname) ?? routes[0],
    [location.pathname],
  );

  return (
    <Layout className="app-shell">
      <Sider className="side-nav" width={264} breakpoint="lg" collapsedWidth={72} theme="light">
        <Link className="brand" to="/" aria-label="GAP Dashboard home">
          <span className="brand-mark" aria-hidden="true">
            <span />
            <span />
            <span />
          </span>
          <span className="brand-copy">
            <strong>GAP</strong>
            <small>Dashboard</small>
          </span>
        </Link>

        <div className="nav-label">Overview</div>
        <Menu
          className="main-menu"
          mode="inline"
          selectedKeys={[location.pathname]}
          items={routes.map((route) => ({
            key: route.path,
            icon: routeIcons[route.path],
            label: <Link to={route.path}>{route.title}</Link>,
          }))}
        />

        <div className="side-nav-footer">
          <span className="status-dot" />
          <span className="side-nav-footer-copy">
            <strong>GAP is running</strong>
            <small>Event delivery dashboard</small>
          </span>
        </div>
      </Sider>

      <Layout className="app-main">
        <Header className="top-nav">
          <div>
            <span className="top-nav-kicker">GAP /</span>
            <strong>{currentRoute.title}</strong>
          </div>
          <div className="top-nav-actions">
            <span className="environment-badge"><span /> Local</span>
            <Tooltip title="Open GAP on GitHub">
              <a
                className="icon-link"
                href="https://github.com/loopopen/gap"
                target="_blank"
                rel="noreferrer"
                aria-label="Open GAP on GitHub"
              >
                <GithubOutlined />
              </a>
            </Tooltip>
          </div>
        </Header>

        <Content className="app-content">
          <main className="page-container">
            <Routes>
              {routes.map((route) => (
                <Route key={route.path} path={route.path} element={<route.component />} />
              ))}
            </Routes>
          </main>
          <footer className="app-footer">
            GAP Dashboard ©{new Date().getFullYear()} · Built by Lopolop Inc.
          </footer>
        </Content>
      </Layout>
    </Layout>
  );
};

const App: React.FC = () => {
  const getBase = () => {
    if (import.meta.env.MODE === 'development') return '';

    const base = document.querySelector('base');
    if (base?.href) {
      const url = new URL(base.href);
      return url.pathname.replace(/\/$/, '');
    }
    return '';
  };

  return (
    <ConfigProvider
      theme={{
        algorithm: theme.defaultAlgorithm,
        token: {
          colorPrimary: '#217f97',
          colorInfo: '#217f97',
          colorSuccess: '#36a269',
          colorWarning: '#d99432',
          colorError: '#d64b4b',
          borderRadius: 8,
          fontFamily: 'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
        },
      }}
    >
      <BrowserRouter basename={getBase()}>
        <AppContent />
      </BrowserRouter>
    </ConfigProvider>
  );
};

export default App;
