import React, { useCallback, useRef, useState } from 'react';
import { ActivityIndicator, Text, TextInput, TouchableOpacity, View } from 'react-native';

import { getActivities } from '../services/alpacaClient';
import { normalizePair } from '../utils/symbols';

export const TxnHistoryCSVViewer = ({ styles }) => {
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState('');
  const [csv, setCsv] = useState('');
  const csvRef = useRef(null);
  const [collapsed, setCollapsed] = useState(false);

  const fetchActivities = useCallback(async ({ days = 7, types = 'FILL,CFEE,FEE,TRANS,PTC', max = 1000 } = {}) => {
    const untilISO = new Date().toISOString();
    const afterISO = new Date(Date.now() - days * 864e5).toISOString();
    let token = null;
    let all = [];
    for (let i = 0; i < 20; i++) {
      const { items, next } = await getActivities({ afterISO, untilISO, pageToken: token, types });
      all = all.concat(items);
      if (!next || all.length >= max) break;
      token = next;
    }
    return all.slice(0, max);
  }, []);

  const toCsv = useCallback((rows) => {
    const header = ['DateTime', 'Type', 'Side', 'Symbol', 'Qty', 'Price', 'CashFlowUSD', 'OrderID', 'ActivityID'];
    const escape = (v) => {
      if (v == null) return '';
      const s = String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const lines = [header.join(',')];
    for (const r of rows) {
      const dtISO = r.transaction_time || r.date || '';
      const local = dtISO ? new Date(dtISO).toLocaleString() : '';
      const side = r.side || '';
      const symbol = normalizePair(r.symbol || '');
      const qty = r.qty || r.cum_qty || '';
      const price = r.price || '';
      let cash = '';
      if ((r.activity_type || '').toUpperCase() === 'FILL') {
        const q = parseFloat(qty ?? '0');
        const p = parseFloat(price ?? '0');
        if (Number.isFinite(q) && Number.isFinite(p)) {
          const signed = q * p * (side === 'buy' ? -1 : 1);
          cash = signed.toFixed(2);
        }
      } else {
        const net = parseFloat(r.net_amount ?? r.amount ?? '');
        cash = Number.isFinite(net) ? net.toFixed(2) : '';
      }
      const row = [local, r.activity_type, side, symbol, qty, price, cash, r.order_id || '', r.id || ''];
      lines.push(row.map(escape).join(','));
    }
    return lines.join('\n');
  }, []);

  const buildRange = async (days) => {
    try {
      setBusy(true);
      setStatus('Fetching…');
      setCsv('');
      setCollapsed(false);
      const acts = await fetchActivities({ days });
      if (!acts.length) {
        setStatus('No activities found in range.');
        return;
      }
      const out = toCsv(acts);
      setCsv(out);
      setStatus(`Built ${acts.length} activities (${days}d). Tap the box → Select All → Copy.`);
      setTimeout(() => {
        try {
          csvRef.current?.focus?.();
          csvRef.current?.setNativeProps?.({ selection: { start: 0, end: out.length } });
        } catch {}
      }, 150);
    } catch (err) {
      setStatus(`Error: ${err.message}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <View style={styles?.txnBox}>
      <Text style={styles?.txnTitle}>Transaction History → CSV</Text>
      <View style={styles?.txnBtnRow}>
        <TouchableOpacity style={styles?.txnBtn} onPress={() => buildRange(1)} disabled={busy}>
          <Text style={styles?.txnBtnText}>Build 24h CSV</Text>
        </TouchableOpacity>
        <TouchableOpacity style={styles?.txnBtn} onPress={() => buildRange(7)} disabled={busy}>
          <Text style={styles?.txnBtnText}>Build 7d CSV</Text>
        </TouchableOpacity>
        <TouchableOpacity style={styles?.txnBtn} onPress={() => buildRange(30)} disabled={busy}>
          <Text style={styles?.txnBtnText}>Build 30d CSV</Text>
        </TouchableOpacity>
      </View>
      {busy ? <ActivityIndicator /> : null}
      <Text style={styles?.txnStatus}>{status}</Text>
      {csv ? (
        <>
          {!collapsed ? (
            <View style={{ marginTop: 8 }}>
              <Text style={styles?.csvHelp}>Tap the box → Select All → Copy</Text>
              <TouchableOpacity onPress={() => setCollapsed(true)} style={styles?.chip}>
                <Text style={styles?.chipText}>Minimize</Text>
              </TouchableOpacity>
              <TextInput
                ref={csvRef}
                style={styles?.csvBox}
                value={csv}
                editable={false}
                multiline
                selectTextOnFocus
                scrollEnabled
                textBreakStrategy="highQuality"
              />
            </View>
          ) : (
            <View style={{ marginTop: 8 }}>
              <TouchableOpacity onPress={() => setCollapsed(false)} style={styles?.chip}>
                <Text style={styles?.chipText}>Show CSV</Text>
              </TouchableOpacity>
            </View>
          )}
        </>
      ) : null}
    </View>
  );
};

export const LiveLogsCopyViewer = ({ styles, logs = [] }) => {
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState('');
  const [txt, setTxt] = useState('');
  const txtRef = useRef(null);
  const [collapsed, setCollapsed] = useState(false);

  const build = async () => {
    try {
      setBusy(true);
      setStatus('Building snapshot…');
      const lines = logs.slice(-200).map((l) => (typeof l === 'string' ? l : JSON.stringify(l)));
      const joined = lines.join('\n');
      setTxt(joined);
      setStatus(`Built ${lines.length} log lines. Tap the box → Select All → Copy.`);
      setTimeout(() => {
        try {
          txtRef.current?.focus?.();
          txtRef.current?.setNativeProps?.({ selection: { start: 0, end: joined.length } });
        } catch {}
      }, 150);
    } catch (err) {
      setStatus(`Error: ${err.message}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <View style={styles?.txnBox}>
      <Text style={styles?.txnTitle}>Live Logs → Copy</Text>
      <View style={styles?.txnBtnRow}>
        <TouchableOpacity style={styles?.txnBtn} onPress={build} disabled={busy}>
          <Text style={styles?.txnBtnText}>Build Snapshot</Text>
        </TouchableOpacity>
      </View>
      {busy ? <ActivityIndicator /> : null}
      <Text style={styles?.txnStatus}>{status}</Text>
      {txt ? (
        <>
          {!collapsed ? (
            <View style={{ marginTop: 8 }}>
              <Text style={styles?.csvHelp}>Tap the box → Select All → Copy</Text>
              <TouchableOpacity onPress={() => setCollapsed(true)} style={styles?.chip}>
                <Text style={styles?.chipText}>Minimize</Text>
              </TouchableOpacity>
              <TextInput
                ref={txtRef}
                style={styles?.csvBox}
                value={txt}
                editable={false}
                multiline
                selectTextOnFocus
                scrollEnabled
                textBreakStrategy="highQuality"
              />
            </View>
          ) : (
            <View style={{ marginTop: 8 }}>
              <TouchableOpacity onPress={() => setCollapsed(false)} style={styles?.chip}>
                <Text style={styles?.chipText}>Show Logs</Text>
              </TouchableOpacity>
            </View>
          )}
        </>
      ) : null}
    </View>
  );
};
