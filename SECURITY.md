# Security Policy

## ⚠️ Project Status

Capsule is currently in **early development (V0 / alpha prototype)**.

This version is intended for:
- Development
- Testing
- Architecture validation
- Community feedback

It is **NOT recommended for production use** or for storing sensitive or critical data.

---

## 🔐 Current Security Characteristics

At this stage, Capsule:

- ✅ Uses SHA-256 for content hashing
- ✅ Performs deterministic chunking and deduplication
- ❌ Does NOT implement encryption at rest
- ❌ Does NOT implement authenticated encryption
- ❌ Does NOT implement integrity verification on restore
- ❌ Does NOT implement access control or authentication
- ❌ Does NOT implement crash-safe atomic writes
- ❌ Has not undergone security audit or formal review

Data stored with this version should be considered **non-confidential and non-critical**.

---

## 🚧 Known Limitations

- Containers are not encrypted.
- Container files may be readable by any user with filesystem access.
- Restore trusts database metadata.
- No tamper detection or MAC validation is implemented.
- Concurrent writes rely on application-level locking.
- The system has not been hardened against malicious input.

---

## 🛣 Planned Security Improvements (Future Versions)

The following are planned for future versions:

- Encryption at rest (AES-GCM or similar)
- Authenticated containers
- Per-container integrity validation
- Crash-safe write pipeline
- Better concurrency safety guarantees
- Optional access control layer
- Streaming restore with integrity verification

---

## 📣 Reporting a Vulnerability

If you discover a security issue, please:

1. Do **NOT** open a public GitHub issue.
2. Contact the maintainer directly via:
   - GitHub private message
   - Or open a security advisory via GitHub's "Report a vulnerability" feature

Please include:
- Clear description of the issue
- Steps to reproduce
- Potential impact
- Suggested remediation (if known)

---

## 🧾 Responsible Disclosure

We ask that security vulnerabilities are disclosed responsibly.
Please allow reasonable time for review and mitigation before public disclosure.

---

## 📌 Disclaimer

Capsule is experimental software provided "as is", without warranty of any kind.
Use at your own risk.

