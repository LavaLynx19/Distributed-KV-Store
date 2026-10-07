# The binary is cross-compiled on the host (harness/docker.sh), so the image
# needs no Go toolchain: just the static binary on a minimal base.
FROM gcr.io/distroless/static-debian12:nonroot
COPY bin/linux/kvnode /kvnode
ENTRYPOINT ["/kvnode"]
