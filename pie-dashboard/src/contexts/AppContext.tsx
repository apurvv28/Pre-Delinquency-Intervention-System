import { createContext, useMemo, useState } from 'react';
import { CustomerRecord, NotificationItem } from '../types';

interface AppContextValue {
  notifications: NotificationItem[];
  unreadAlerts: number;
  pushNotification: (notification: Omit<NotificationItem, 'id' | 'createdAt' | 'read'>) => void;
  markNotificationRead: (id: string) => void;
  activeCustomer: CustomerRecord | null;
  setActiveCustomer: (value: CustomerRecord | null) => void;
}

export const AppContext = createContext<AppContextValue | null>(null);

function makeId(prefix: string): string {
  return `${prefix}_${Math.random().toString(36).slice(2, 10)}`;
}

export function AppProvider({ children }: { children: React.ReactNode }) {
  const [notifications, setNotifications] = useState<NotificationItem[]>([]);
  const [activeCustomer, setActiveCustomer] = useState<CustomerRecord | null>(null);

  const pushNotification = (notification: Omit<NotificationItem, 'id' | 'createdAt' | 'read'>) => {
    setNotifications((prev) => [
      {
        id: makeId('note'),
        createdAt: new Date().toISOString(),
        read: false,
        ...notification,
      },
      ...prev,
    ]);
  };

  const markNotificationRead = (id: string) => {
    setNotifications((prev) => prev.map((item) => (item.id === id ? { ...item, read: true } : item)));
  };

  const value = useMemo<AppContextValue>(
    () => ({
      notifications,
      unreadAlerts: notifications.filter((item) => !item.read).length,
      pushNotification,
      markNotificationRead,
      activeCustomer,
      setActiveCustomer,
    }),
    [notifications, activeCustomer],
  );

  return <AppContext.Provider value={value}>{children}</AppContext.Provider>;
}
