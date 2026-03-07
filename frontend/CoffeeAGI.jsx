import { useState, useRef, useEffect } from "react";

const API = "http://localhost:8000";

const DIFFICULTIES = ["foundation", "intermediate", "advanced", "expert"];
const TOPICS = [
  "Coffee Origins & Agriculture",
  "Post-Harvest Processing",
  "Green Coffee Evaluation",
  "Roasting Science",
  "Sensory Skills & Cupping",
  "Brewing & Extraction",
  "Espresso Mastery",
  "Advanced Barista Skills",
  "Coffee Business & Operations",
];

// ── Helpers ──────────────────────────────────────────────────────────────────

async function api(method, path, body, isForm = false) {
  const opts = { method };
  if (body) {
    if (isForm) {
      opts.body = body;
    } else {
      opts.headers = { "Content-Type": "application/json" };
      opts.body = JSON.stringify(body);
    }
  }
  const res = await fetch(API + path, opts);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || "Request failed");
  }
  return res.json();
}

// ── Components ────────────────────────────────────────────────────────────────

function Badge({ color, children }) {
  const palette = {
    brown: "bg-amber-800 text-amber-50",
    green: "bg-emerald-700 text-white",
    red: "bg-red-600 text-white",
    gold: "bg-yellow-600 text-white",
    gray: "bg-gray-500 text-white",
  };
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${palette[color] || palette.gray}`}>
      {children}
    </span>
  );
}

function Spinner() {
  return (
    <svg className="animate-spin h-5 w-5 text-amber-700" viewBox="0 0 24 24" fill="none">
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z" />
    </svg>
  );
}

// ── Chat Tab ──────────────────────────────────────────────────────────────────

function ChatTab() {
  const [messages, setMessages] = useState([
    { role: "assistant", text: "Hello! I'm Coffee AGI, your specialty coffee expert. Ask me anything — origins, roasting, extraction, sensory skills, or anything in between." },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [sessionId] = useState(() => `sess_${Date.now()}`);
  const bottomRef = useRef(null);

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: "smooth" }); }, [messages]);

  async function send() {
    if (!input.trim() || loading) return;
    const msg = input.trim();
    setInput("");
    setMessages((m) => [...m, { role: "user", text: msg }]);
    setLoading(true);
    try {
      const data = await api("POST", "/chat", { message: msg, session_id: sessionId });
      setMessages((m) => [...m, { role: "assistant", text: data.response }]);
    } catch (e) {
      setMessages((m) => [...m, { role: "assistant", text: `Error: ${e.message}` }]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-y-auto space-y-4 p-4">
        {messages.map((m, i) => (
          <div key={i} className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
            <div
              className={`max-w-2xl rounded-2xl px-4 py-3 text-sm leading-relaxed whitespace-pre-wrap ${
                m.role === "user"
                  ? "bg-amber-800 text-amber-50 rounded-br-sm"
                  : "bg-amber-50 border border-amber-200 text-amber-900 rounded-bl-sm"
              }`}
            >
              {m.text}
            </div>
          </div>
        ))}
        {loading && (
          <div className="flex justify-start">
            <div className="bg-amber-50 border border-amber-200 rounded-2xl rounded-bl-sm px-4 py-3">
              <Spinner />
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>
      <div className="border-t border-amber-200 p-4 flex gap-2">
        <input
          className="flex-1 border border-amber-300 rounded-xl px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-600 bg-amber-50"
          placeholder="Ask anything about specialty coffee..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && send()}
        />
        <button
          onClick={send}
          disabled={loading}
          className="bg-amber-800 text-white px-5 py-2 rounded-xl text-sm font-semibold hover:bg-amber-700 disabled:opacity-50 transition-colors"
        >
          Send
        </button>
      </div>
    </div>
  );
}

// ── Curriculum Tab ────────────────────────────────────────────────────────────

function CurriculumTab() {
  const [file, setFile] = useState(null);
  const [topic, setTopic] = useState("general");
  const [difficulty, setDifficulty] = useState("foundation");
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(false);
  const [topics, setTopics] = useState(null);

  useEffect(() => {
    api("GET", "/knowledge/topics")
      .then(setTopics)
      .catch(() => {});
  }, [status]);

  async function upload() {
    if (!file) return;
    setLoading(true);
    setStatus(null);
    try {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("topic_tag", topic);
      fd.append("difficulty_level", difficulty);
      const data = await api("POST", "/knowledge/ingest", fd, true);
      setStatus({ ok: true, msg: `Ingested ${data.chunks} chunks from "${data.filename}"` });
      setFile(null);
    } catch (e) {
      setStatus({ ok: false, msg: e.message });
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="p-6 max-w-2xl mx-auto space-y-6">
      <div className="bg-amber-50 border border-amber-200 rounded-2xl p-5 space-y-4">
        <h2 className="font-bold text-amber-900 text-lg">Upload Curriculum Material</h2>
        <p className="text-sm text-amber-700">Supported: PDF, PPTX, TXT, Markdown</p>

        <div
          className="border-2 border-dashed border-amber-300 rounded-xl p-6 text-center cursor-pointer hover:border-amber-500 transition-colors"
          onClick={() => document.getElementById("file-input").click()}
        >
          {file ? (
            <p className="text-amber-800 font-medium">{file.name}</p>
          ) : (
            <p className="text-amber-600 text-sm">Click to choose a file</p>
          )}
          <input
            id="file-input"
            type="file"
            accept=".pdf,.pptx,.ppt,.txt,.md"
            className="hidden"
            onChange={(e) => setFile(e.target.files[0])}
          />
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="text-xs font-semibold text-amber-800 block mb-1">Topic Tag</label>
            <input
              className="w-full border border-amber-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-500"
              value={topic}
              onChange={(e) => setTopic(e.target.value)}
              placeholder="e.g. roasting, espresso"
            />
          </div>
          <div>
            <label className="text-xs font-semibold text-amber-800 block mb-1">Difficulty</label>
            <select
              className="w-full border border-amber-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-500"
              value={difficulty}
              onChange={(e) => setDifficulty(e.target.value)}
            >
              {DIFFICULTIES.map((d) => (
                <option key={d} value={d}>{d.charAt(0).toUpperCase() + d.slice(1)}</option>
              ))}
            </select>
          </div>
        </div>

        <button
          onClick={upload}
          disabled={!file || loading}
          className="w-full bg-amber-800 text-white py-2.5 rounded-xl font-semibold hover:bg-amber-700 disabled:opacity-50 transition-colors flex items-center justify-center gap-2"
        >
          {loading ? <><Spinner /> Ingesting...</> : "Upload to Knowledge Base"}
        </button>

        {status && (
          <div className={`rounded-lg px-4 py-2 text-sm ${status.ok ? "bg-emerald-50 text-emerald-800 border border-emerald-200" : "bg-red-50 text-red-800 border border-red-200"}`}>
            {status.msg}
          </div>
        )}
      </div>

      {topics && topics.total_chunks > 0 && (
        <div className="bg-amber-50 border border-amber-200 rounded-2xl p-5">
          <h3 className="font-bold text-amber-900 mb-3">Knowledge Base ({topics.total_chunks} chunks)</h3>
          <div className="flex flex-wrap gap-2">
            {Object.entries(topics.topics).map(([t, count]) => (
              <span key={t} className="bg-amber-800 text-amber-50 text-xs px-3 py-1 rounded-full">
                {t} ({count})
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Test Tab ──────────────────────────────────────────────────────────────────

function TestTab() {
  const [step, setStep] = useState("setup");
  const [config, setConfig] = useState({ topic: TOPICS[0], difficulty: "foundation", numQ: 10, email: "", name: "" });
  const [test, setTest] = useState(null);
  const [answers, setAnswers] = useState({});
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  async function generateTest() {
    if (!config.email.trim() || !config.name.trim()) {
      setError("Please enter your name and email.");
      return;
    }
    setError(null);
    setLoading(true);
    try {
      const data = await api("POST", "/tests/generate", {
        topic: config.topic,
        difficulty: config.difficulty,
        num_questions: config.numQ,
        student_email: config.email,
      });
      setTest(data);
      setAnswers({});
      setStep("exam");
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  async function submitTest() {
    setLoading(true);
    setError(null);
    try {
      const data = await api("POST", "/tests/submit", {
        test_id: test.test_id,
        student_email: config.email,
        student_name: config.name,
        answers,
      });
      setResult(data);
      setStep("result");
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  if (step === "setup") {
    return (
      <div className="p-6 max-w-lg mx-auto space-y-5">
        <div className="bg-amber-50 border border-amber-200 rounded-2xl p-5 space-y-4">
          <h2 className="font-bold text-amber-900 text-lg">Generate Exam</h2>

          {[
            { label: "Your Name", key: "name", type: "text", placeholder: "Full name" },
            { label: "Email", key: "email", type: "email", placeholder: "your@email.com" },
          ].map(({ label, key, type, placeholder }) => (
            <div key={key}>
              <label className="text-xs font-semibold text-amber-800 block mb-1">{label}</label>
              <input
                type={type}
                className="w-full border border-amber-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-500"
                placeholder={placeholder}
                value={config[key]}
                onChange={(e) => setConfig((c) => ({ ...c, [key]: e.target.value }))}
              />
            </div>
          ))}

          <div>
            <label className="text-xs font-semibold text-amber-800 block mb-1">Topic</label>
            <select
              className="w-full border border-amber-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-500"
              value={config.topic}
              onChange={(e) => setConfig((c) => ({ ...c, topic: e.target.value }))}
            >
              {TOPICS.map((t) => <option key={t}>{t}</option>)}
            </select>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-xs font-semibold text-amber-800 block mb-1">Difficulty</label>
              <select
                className="w-full border border-amber-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-500"
                value={config.difficulty}
                onChange={(e) => setConfig((c) => ({ ...c, difficulty: e.target.value }))}
              >
                {DIFFICULTIES.map((d) => <option key={d}>{d}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs font-semibold text-amber-800 block mb-1">Questions</label>
              <input
                type="number"
                min={3} max={30}
                className="w-full border border-amber-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-500"
                value={config.numQ}
                onChange={(e) => setConfig((c) => ({ ...c, numQ: parseInt(e.target.value) || 10 }))}
              />
            </div>
          </div>

          {error && <p className="text-red-600 text-sm">{error}</p>}

          <button
            onClick={generateTest}
            disabled={loading}
            className="w-full bg-amber-800 text-white py-2.5 rounded-xl font-semibold hover:bg-amber-700 disabled:opacity-50 transition-colors flex items-center justify-center gap-2"
          >
            {loading ? <><Spinner /> Generating exam...</> : "Generate Exam"}
          </button>
        </div>
      </div>
    );
  }

  if (step === "exam" && test) {
    const answered = Object.keys(answers).length;
    const total = test.questions.length;

    return (
      <div className="p-6 max-w-2xl mx-auto space-y-5">
        <div className="bg-amber-800 text-amber-50 rounded-2xl p-4 flex items-center justify-between">
          <div>
            <p className="font-bold">{test.topic}</p>
            <p className="text-xs text-amber-200">{test.difficulty} · {total} questions</p>
          </div>
          <p className="text-sm">{answered}/{total} answered</p>
        </div>

        {test.questions.map((q) => (
          <div key={q.id} className="bg-amber-50 border border-amber-200 rounded-2xl p-4 space-y-3">
            <div className="flex items-start gap-2">
              <Badge color="brown">{q.type}</Badge>
              <p className="text-sm font-medium text-amber-900 flex-1">
                {q.id}. {q.question}
              </p>
            </div>

            {q.type === "MC" && (
              <div className="space-y-2">
                {Object.entries(q.options).map(([k, v]) => (
                  <label key={k} className="flex items-center gap-2 cursor-pointer group">
                    <input
                      type="radio"
                      name={`q_${q.id}`}
                      value={k}
                      checked={answers[q.id] === k}
                      onChange={() => setAnswers((a) => ({ ...a, [q.id]: k }))}
                      className="accent-amber-700"
                    />
                    <span className="text-sm text-amber-800 group-hover:text-amber-900">
                      <strong>{k}.</strong> {v}
                    </span>
                  </label>
                ))}
              </div>
            )}

            {q.type === "TF" && (
              <div className="flex gap-4">
                {["True", "False"].map((opt) => (
                  <label key={opt} className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="radio"
                      name={`q_${q.id}`}
                      value={opt}
                      checked={answers[q.id] === opt}
                      onChange={() => setAnswers((a) => ({ ...a, [q.id]: opt }))}
                      className="accent-amber-700"
                    />
                    <span className="text-sm text-amber-800">{opt}</span>
                  </label>
                ))}
              </div>
            )}

            {q.type === "SA" && (
              <textarea
                rows={3}
                className="w-full border border-amber-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-500 resize-none"
                placeholder="Write your answer here..."
                value={answers[q.id] || ""}
                onChange={(e) => setAnswers((a) => ({ ...a, [q.id]: e.target.value }))}
              />
            )}
          </div>
        ))}

        {error && <p className="text-red-600 text-sm text-center">{error}</p>}

        <button
          onClick={submitTest}
          disabled={loading}
          className="w-full bg-amber-800 text-white py-3 rounded-xl font-semibold hover:bg-amber-700 disabled:opacity-50 transition-colors flex items-center justify-center gap-2"
        >
          {loading ? <><Spinner /> Grading...</> : `Submit Exam (${answered}/${total} answered)`}
        </button>
      </div>
    );
  }

  if (step === "result" && result) {
    return (
      <div className="p-6 max-w-2xl mx-auto space-y-5">
        <div className={`rounded-2xl p-5 text-center ${result.passed ? "bg-emerald-50 border-2 border-emerald-300" : "bg-red-50 border-2 border-red-200"}`}>
          <p className="text-4xl font-black mb-1" style={{ color: result.passed ? "#065f46" : "#991b1b" }}>
            {result.score}%
          </p>
          <p className={`font-bold text-lg ${result.passed ? "text-emerald-800" : "text-red-700"}`}>
            {result.passed ? "PASSED" : "NOT PASSED"} — passing score: {result.passing_score}%
          </p>
          <p className="text-sm mt-1 text-gray-600">{result.topic} · {result.difficulty}</p>
          {result.passed && (
            <p className="mt-2 text-emerald-700 text-sm font-medium">
              Certificate earned: {result.certificate_track}
            </p>
          )}
        </div>

        {result.certificate && (
          <div className="bg-yellow-50 border border-yellow-300 rounded-2xl p-4 flex items-center justify-between">
            <div>
              <p className="font-bold text-yellow-800">Certificate Generated</p>
              <p className="text-xs text-yellow-700">ID: {result.certificate.id?.slice(0, 8)}...</p>
            </div>
            <a
              href={`${API}/certificates/${result.certificate.id}/download`}
              target="_blank"
              rel="noreferrer"
              className="bg-yellow-600 text-white px-4 py-2 rounded-lg text-sm font-semibold hover:bg-yellow-500 transition-colors"
            >
              Download PDF
            </a>
          </div>
        )}

        <div className="space-y-3">
          <h3 className="font-bold text-amber-900">Question Review</h3>
          {result.graded_questions.map((g) => (
            <div
              key={g.id}
              className={`rounded-xl p-3 border text-sm space-y-1 ${
                g.type === "SA"
                  ? "bg-blue-50 border-blue-200"
                  : g.correct
                  ? "bg-emerald-50 border-emerald-200"
                  : "bg-red-50 border-red-200"
              }`}
            >
              <div className="flex items-center gap-2">
                <Badge color={g.type === "SA" ? "gray" : g.correct ? "green" : "red"}>
                  {g.type === "SA" ? `SA: ${g.score}/10` : g.correct ? "Correct" : "Wrong"}
                </Badge>
                <span className="text-gray-700 font-medium">Q{g.id}</span>
              </div>
              <p className="text-gray-800">{g.question}</p>
              {g.type !== "SA" && !g.correct && (
                <p className="text-emerald-700 text-xs">Correct: {g.correct_answer}</p>
              )}
              {g.type === "SA" && g.feedback && (
                <p className="text-blue-700 text-xs">{g.feedback}</p>
              )}
              {g.explanation && (
                <p className="text-gray-500 text-xs italic">{g.explanation}</p>
              )}
            </div>
          ))}
        </div>

        <button
          onClick={() => { setStep("setup"); setTest(null); setResult(null); }}
          className="w-full border border-amber-700 text-amber-800 py-2.5 rounded-xl font-semibold hover:bg-amber-50 transition-colors"
        >
          Take Another Exam
        </button>
      </div>
    );
  }

  return null;
}

// ── Certificates Tab ──────────────────────────────────────────────────────────

function CertificatesTab() {
  const [email, setEmail] = useState("");
  const [certs, setCerts] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  async function lookup() {
    if (!email.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const data = await api("GET", `/certificates/student/${encodeURIComponent(email)}`);
      setCerts(data.certificates);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="p-6 max-w-lg mx-auto space-y-5">
      <div className="bg-amber-50 border border-amber-200 rounded-2xl p-5 space-y-3">
        <h2 className="font-bold text-amber-900 text-lg">My Certificates</h2>
        <div className="flex gap-2">
          <input
            type="email"
            className="flex-1 border border-amber-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-500"
            placeholder="your@email.com"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && lookup()}
          />
          <button
            onClick={lookup}
            disabled={loading}
            className="bg-amber-800 text-white px-4 py-2 rounded-lg text-sm font-semibold hover:bg-amber-700 disabled:opacity-50 transition-colors"
          >
            {loading ? "..." : "Look up"}
          </button>
        </div>
        {error && <p className="text-red-600 text-sm">{error}</p>}
      </div>

      {certs !== null && (
        certs.length === 0 ? (
          <p className="text-center text-amber-700 text-sm py-8">No certificates found for this email.</p>
        ) : (
          <div className="space-y-3">
            {certs.map((cert) => (
              <div key={cert.id} className="bg-amber-50 border border-amber-200 rounded-2xl p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="font-bold text-amber-900">{cert.certificate_track}</p>
                    <p className="text-xs text-amber-700 mt-0.5">{cert.topic} · Score: {cert.score}%</p>
                    <p className="text-xs text-amber-600 mt-0.5">Issued: {cert.issued_date}</p>
                    <p className="text-xs text-gray-400 mt-0.5">ID: {cert.id?.slice(0, 8)}...</p>
                  </div>
                  <a
                    href={`${API}/certificates/${cert.id}/download`}
                    target="_blank"
                    rel="noreferrer"
                    className="bg-amber-800 text-white px-3 py-1.5 rounded-lg text-xs font-semibold hover:bg-amber-700 transition-colors whitespace-nowrap"
                  >
                    Download PDF
                  </a>
                </div>
              </div>
            ))}
          </div>
        )
      )}
    </div>
  );
}

// ── App Shell ─────────────────────────────────────────────────────────────────

const TABS = [
  { id: "chat", label: "Chat" },
  { id: "curriculum", label: "Curriculum" },
  { id: "exam", label: "Take Exam" },
  { id: "certificates", label: "Certificates" },
];

export default function CoffeeAGI() {
  const [tab, setTab] = useState("chat");

  return (
    <div className="min-h-screen bg-stone-100 font-sans">
      {/* Header */}
      <header className="bg-amber-900 text-amber-50 px-6 py-4 flex items-center justify-between shadow-md">
        <div>
          <h1 className="text-xl font-black tracking-tight">Coffee AGI</h1>
          <p className="text-xs text-amber-300">Maillard Coffee Roasters · Seed to Cup Intelligence</p>
        </div>
        <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" title="API online" />
      </header>

      {/* Tabs */}
      <nav className="bg-white border-b border-amber-200 px-4 flex gap-1">
        {TABS.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-3 text-sm font-semibold border-b-2 transition-colors ${
              tab === t.id
                ? "border-amber-800 text-amber-900"
                : "border-transparent text-gray-500 hover:text-amber-800"
            }`}
          >
            {t.label}
          </button>
        ))}
      </nav>

      {/* Content */}
      <main className="h-[calc(100vh-108px)] overflow-y-auto">
        {tab === "chat" && <div className="h-full flex flex-col"><ChatTab /></div>}
        {tab === "curriculum" && <CurriculumTab />}
        {tab === "exam" && <TestTab />}
        {tab === "certificates" && <CertificatesTab />}
      </main>
    </div>
  );
}
