import type { ComponentType, ReactNode } from 'react'
import { FiHome, FiInfo, FiMail } from 'react-icons/fi'

import Message from '../pages/Message'
import About from '../pages/About'
import Home from '../pages/Home'

export interface RouteConfig {
  path: string
  title: string
  component: ComponentType
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
    title: 'Messages',
  },
  {
    path: '/about',
    component: About,
    title: 'About',
  },
]

export interface NavItem {
  path: string
  label: string
  icon: ReactNode
}

export const navSections: { sectionLabel?: string; items: NavItem[] }[] = [
  {
    items: [{ path: '/', label: 'Home', icon: <FiHome size={20} /> }],
  },
  {
    sectionLabel: 'Data',
    items: [{ path: '/message', label: 'Messages', icon: <FiMail size={20} /> }],
  },
  {
    items: [{ path: '/about', label: 'About', icon: <FiInfo size={20} /> }],
  },
]
