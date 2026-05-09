const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

export async function getHealth() {
  const response = await fetch(`${API_BASE_URL}/api/health`);
  if (!response.ok) throw new Error('Backend health check failed');
  return response.json();
}

export async function listSessions() {
  const response = await fetch(`${API_BASE_URL}/api/sessions`);
  if (!response.ok) throw new Error('Could not load sessions');
  return response.json();
}

export async function getSession(sessionId) {
  const response = await fetch(`${API_BASE_URL}/api/sessions/${sessionId}`);
  if (!response.ok) throw new Error('Could not load session');
  return response.json();
}

export async function deleteSession(sessionId) {
  const response = await fetch(`${API_BASE_URL}/api/sessions/${sessionId}`, {
    method: 'DELETE',
  });
  if (!response.ok) throw new Error('Could not delete session');
}

export async function sendConsultantMessage({ sessionId, message, model }) {
  const response = await fetch(`${API_BASE_URL}/api/sessions/${sessionId}/chat`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ message, model: model || null }),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.detail || 'Consultant chat failed');
  }

  return response.json();
}

export async function analyzePdf({ file }) {
  const body = new FormData();
  body.append('file', file);

  const response = await fetch(`${API_BASE_URL}/api/analyze-pdf`, {
    method: 'POST',
    body,
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.detail || 'Analysis failed');
  }

  return response.json();
}

export function absoluteApiUrl(path) {
  if (!path) return '#';
  if (path.startsWith('http')) return path;
  return `${API_BASE_URL}${path}`;
}
