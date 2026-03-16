import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import axios from 'axios'

declare global {
  interface Window {
    baseUrl: string;
  }
}

let baseURL = "";
switch (import.meta.env.MODE) {
  case 'development':
    baseURL = '/gap-dashboard/api';
    break
  default:
    baseURL = window.baseUrl;
    break
}

axios.defaults.baseURL = baseURL;
axios.defaults.headers.post['Content-Type'] = 'application/json';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
