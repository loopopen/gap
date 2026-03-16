import type { ComponentType } from 'react';

import Message from '../pages/Message';
import About from '../pages/About';
import Home from '../pages/Home';

export interface RouteConfig {
  path: string;
  title: string;
  component: ComponentType;
}

export const routes: RouteConfig[] = [
  {
    path: '/',
    component: Home,
    title: 'Home',
  },
  {
    path: '/message',
    component: Message,
    title: 'Message',
  },
  {
    path: '/about',
    component: About,
    title: 'About',
  }
];

export const menuItems = routes.map(route => ({
  label: route.title,
  path: route.path
}));