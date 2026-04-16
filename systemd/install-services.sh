#!/bin/bash
# Install WxStream systemd services
# Run as root from the wxstream-rs directory

UNIT_DIR=/etc/systemd/system
SERVICE_DIR="$(dirname "$0")/systemd"

echo "Installing WxStream systemd services..."

cp "$SERVICE_DIR"/wxstream-*.service "$UNIT_DIR"/

systemctl daemon-reload

echo ""
echo "Services installed. To enable and start:"
echo ""
echo "  # Scanner (always run exactly one)"
echo "  systemctl enable --now wxstream-scanner"
echo ""
echo "  # Workers with default concurrency from config.toml"
echo "  systemctl enable --now wxstream-transcribe"
echo "  systemctl enable --now wxstream-parse"
echo "  systemctl enable --now wxstream-trim"
echo "  systemctl enable --now wxstream-quality"
echo ""
echo "  # Workers with explicit concurrency (template units)"
echo "  systemctl enable --now wxstream-transcribe@20"
echo "  systemctl enable --now wxstream-trim@10"
echo "  systemctl enable --now wxstream-parse@5"
echo ""
echo "  # View logs"
echo "  journalctl -u wxstream-transcribe -f"
echo "  journalctl -u wxstream-trim -f"
echo ""
echo "  # Status of all wxstream services"
echo "  systemctl status 'wxstream-*'"
