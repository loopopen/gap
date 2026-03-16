import React from 'react';
import { Layout, Menu, theme } from 'antd';
import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom';
import "./App.css";
import { routes, menuItems } from './config/routes';

const { Header, Content, Footer } = Layout;

const AppContent: React.FC = () => {
  const location = useLocation();
  const {
    token: { colorBgContainer, borderRadiusLG },
  } = theme.useToken();

  return (
    <Layout>
      <Header style={{ display: 'flex', alignItems: 'center' }}>
        <div className="logo">
          <span>GAP Dashboard</span>
        </div>
        <Menu
          theme="dark"
          mode="horizontal"
          selectedKeys={[location.pathname]}
          items={menuItems.map(item => ({
            key: item.path,
            label: <Link to={item.path}>{item.label}</Link>
          }))}
          style={{ flex: 1, minWidth: 0 }}
        />
      </Header>
      <Content>
        <div
          style={{
            background: colorBgContainer,
            minHeight: 280,
            padding: 24,
            borderRadius: borderRadiusLG,
          }}
        >
          <Routes>
            {routes.map(route => (
              <Route
                key={route.path}
                path={route.path}
                element={<route.component />}
              />
            ))}
          </Routes>
        </div>
      </Content>
      <Footer style={{ textAlign: 'center' }}>
        GAP Dashboard ©{new Date().getFullYear()} Created by Lopolop Inc.
      </Footer>
    </Layout>
  );
};

const App: React.FC = () => {
  const getBase = () => {
    switch (import.meta.env.MODE) {
      case 'development':
        return ''
      default:
    }

    const base = document.querySelector('base');
    if (base && base.href) {
      const url = new URL(base.href);
      return url.pathname.replace(/\/$/, '');
    }
    return '';
  };

  return (
    <BrowserRouter basename={getBase()}>
      <AppContent />
    </BrowserRouter>
  );
};

export default App;