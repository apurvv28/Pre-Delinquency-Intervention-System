import { useEffect, useRef, useCallback, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';

export type StreamEvent = 
  | {
      type: 'transaction';
      data: {
        customer_id: string;
        transaction_index: number;
        amount: number;
        balance_after: number;
        days_since_last_payment: number;
        merchant_category: string;
        is_international: boolean;
        transaction_time: string;
        risk_score: number;
        risk_bucket: string;
      };
    }
  | {
      type: 'risk_score_update';
      data: {
        customer_id: string;
        risk_score: number;
        risk_bucket: string;
        timestamp: string;
      };
    }
  | {
      type: 'model_output';
      data: {
        event_type: string;
        [key: string]: unknown;
      };
    };

export function useStreamWebSocket(onEvent?: (event: StreamEvent) => void) {
  const wsRef = useRef<WebSocket | null>(null);
  const queryClient = useQueryClient();
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const invalidateTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pendingCustomerIdsRef = useRef<Set<string>>(new Set());
  const [isConnected, setIsConnected] = useState(false);
  const invalidateDebounceMs = Number(import.meta.env.VITE_WS_INVALIDATE_DEBOUNCE_MS ?? 2000);

  const scheduleInvalidation = useCallback((customerId?: string) => {
    if (customerId) {
      pendingCustomerIdsRef.current.add(customerId);
    }

    if (invalidateTimeoutRef.current) {
      return;
    }

    invalidateTimeoutRef.current = setTimeout(() => {
      queryClient.invalidateQueries({ queryKey: ['registryDashboard'] });
      queryClient.invalidateQueries({ queryKey: ['customersDashboard'] });
      queryClient.invalidateQueries({ queryKey: ['metricSnapshot'] });

      pendingCustomerIdsRef.current.forEach((id) => {
        queryClient.invalidateQueries({ queryKey: ['customer', id] });
      });
      pendingCustomerIdsRef.current.clear();

      invalidateTimeoutRef.current = null;
    }, Math.max(500, invalidateDebounceMs));
  }, [invalidateDebounceMs, queryClient]);

  const resolveWebSocketUrl = () => {
    const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000';
    const baseUrl = new URL(apiBaseUrl);
    baseUrl.protocol = baseUrl.protocol === 'https:' ? 'wss:' : 'ws:';
    baseUrl.pathname = '/ws/stream';
    baseUrl.search = '';
    baseUrl.hash = '';
    return baseUrl.toString();
  };

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN || wsRef.current?.readyState === WebSocket.CONNECTING) {
      return;
    }

    try {
      const wsUrl = resolveWebSocketUrl();
      
      console.log('[WS] Connecting to:', wsUrl);
      const ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        console.log('[WS] Connected');
        setIsConnected(true);
        // Clear any pending reconnect timeout
        if (reconnectTimeoutRef.current) {
          clearTimeout(reconnectTimeoutRef.current);
          reconnectTimeoutRef.current = null;
        }
      };

      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data) as StreamEvent;
          console.log('[WS] Received:', message.type);

          // Call the handler if provided
          if (onEvent) {
            onEvent(message);
          }

          // Coalesce refetches to avoid overwhelming the UI during fast streams.
          if (message.type === 'transaction') {
            scheduleInvalidation();
          } else if (message.type === 'risk_score_update') {
            scheduleInvalidation(message.data.customer_id);
          }
        } catch (err) {
          console.error('[WS] Failed to parse message:', err);
        }
      };

      ws.onerror = (error) => {
        console.error('[WS] Error:', error);
        setIsConnected(false);
      };

      ws.onclose = () => {
        console.log('[WS] Disconnected, attempting reconnect...');
        setIsConnected(false);
        wsRef.current = null;
        
        // Schedule reconnect attempt in 3 seconds
        reconnectTimeoutRef.current = setTimeout(() => {
          connect();
        }, 3000);
      };

      wsRef.current = ws;
    } catch (err) {
      console.error('[WS] Failed to create WebSocket:', err);
    }
  }, [onEvent, queryClient]);

  useEffect(() => {
    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (invalidateTimeoutRef.current) {
        clearTimeout(invalidateTimeoutRef.current);
        invalidateTimeoutRef.current = null;
      }
      pendingCustomerIdsRef.current.clear();
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      setIsConnected(false);
    };
  }, [connect]);

  return {
    isConnected,
  };
}
