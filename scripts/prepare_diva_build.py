#!/usr/bin/env python3
"""Inject the DIVA HTTP forward call into the build workspace.

This keeps the experimental adapter change small while the fork is still being
validated. The generated container contains the call; the script is idempotent.
"""

from pathlib import Path

PATH = Path("pkg/connector/handle_message.go")

needle = '''\t\tlc.UserLogin.Bridge.Log.Info().
\t\t\tStr("diva_event", "DIVA_RX").
\t\t\tStr("text", unwrappedText).
\t\t\tStr("group_id", portalIDStr).
\t\t\tStr("sender_id", msg.From).
\t\t\tStr("message_id", msg.ID).
\t\t\tBool("decryption_failed", decryptionFailed).
\t\t\tMsg("[DIVA_RX]")
'''

replacement = needle + '''\n\t\tlc.forwardDIVAInbound(unwrappedText, portalIDStr, msg.From, msg.ID)\n'''

text = PATH.read_text(encoding="utf-8")
if "lc.forwardDIVAInbound(unwrappedText, portalIDStr, msg.From, msg.ID)" in text:
    print("DIVA adapter call already present")
else:
    if needle not in text:
        raise SystemExit("Could not find DIVA_RX block to patch")
    PATH.write_text(text.replace(needle, replacement, 1), encoding="utf-8")
    print("Injected DIVA HTTP adapter call")
