import { useEffect } from 'react';
import { Navigate } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth';

export default function Login() {
  const { isAuthenticated, loginWithGoogle } = useAuth();

  useEffect(() => {
    document.title = 'PIE Admin Login';
  }, []);

  if (isAuthenticated) {
    return <Navigate to="/" replace />;
  }

  return (
    <main className="relative flex min-h-screen items-center justify-center bg-[#F4F6F9] px-6">
      <section className="z-10 w-full max-w-xl rounded-lg border border-[#E2E6ED] bg-white p-8 shadow-sm">
        <p className="text-xs uppercase tracking-[0.18em] text-[#0057B8]">Pre-Delinquency Intelligence Engine</p>
        <h1 className="mt-3 font-syne text-4xl text-[#003366]">PIE Command Access</h1>
        <p className="mt-3 text-sm leading-6 text-[#475569]">
          Authorized bank administrators only. Access is monitored and all high-risk operations are audit logged.
        </p>
        <button
          onClick={loginWithGoogle}
          className="mt-6 w-full rounded-md border border-[#003366] bg-white px-5 py-3 font-semibold text-[#003366] hover:bg-[#F4F6F9]"
        >
          Sign in with Google
        </button>
        <p className="mt-6 text-center text-xs text-[#94A3B8]">
          Unauthorized use is prohibited. By continuing, you agree to internal compliance and monitoring policies.
        </p>
      </section>
    </main>
  );
}


