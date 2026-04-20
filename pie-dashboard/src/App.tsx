import { BrowserRouter, Navigate, Route, Routes, useLocation } from 'react-router-dom';
import Sidebar from './components/Sidebar';
import TopBar from './components/TopBar';
import ProtectedRoute from './components/ProtectedRoute';
import Dashboard from './pages/Dashboard';
import Login from './pages/Login';
import ModelInsights from './pages/ModelInsights';
import Predict from './pages/Predict';
import CustomerLookup from './pages/CustomerLookup';
import Interventions from './pages/Interventions';
import Registry from './pages/Registry';
import Settings from './pages/Settings';

const titleMap: Record<string, string> = {
  '/': 'Risk Command Center',
  '/customers': 'Customer Search',
  '/predict': 'Loan Defaulter Predictor',
  '/registry': 'Customer Risk Registry',
  '/interventions': 'Intervention Operations',
  '/insights': 'Model Insights',
  '/settings': 'Settings & Admin',
};

function ShellLayout({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  const title = titleMap[location.pathname] ?? 'PIE';

  return (
    <div className="relative flex min-h-screen overflow-hidden bg-[#F4F6F9] text-[#0F172A]">
      <Sidebar />
      <div className="relative z-10 flex min-w-0 flex-1 flex-col">
        <TopBar title={title} />
        <main className="flex-1 overflow-auto">{children}</main>
      </div>
    </div>
  );
}

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route
          path="*"
          element={
            <ProtectedRoute>
              <ShellLayout>
                <Routes>
                  <Route path="/" element={<Dashboard />} />
                  <Route path="/customers" element={<CustomerLookup />} />
                  <Route path="/predict" element={<Predict />} />
                  <Route path="/registry" element={<Registry />} />
                  <Route path="/interventions" element={<Interventions />} />
                  <Route path="/insights" element={<ModelInsights />} />
                  <Route path="/settings" element={<Settings />} />
                  <Route path="*" element={<Navigate to="/" replace />} />
                </Routes>
              </ShellLayout>
            </ProtectedRoute>
          }
        />
      </Routes>
    </BrowserRouter>
  );
}

export default App;