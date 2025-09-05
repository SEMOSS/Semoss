import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';

// This is the root file of the React app. The code below renders <App /> inside of the <div id="root" />
createRoot(document.getElementById('root')).render(
    <StrictMode>
        <App />
    </StrictMode>,
);
