# 真实 sshd 测试镜像（T4）：容器内运行 `cargo test -p termx-ssh`，SSH 连 localhost。
#
# 仅用于测试：密码为固定测试值、主机密钥在构建时生成；测试客户端显式接受任意主机密钥。
# 容器生命周期由运行命令控制（`docker run --rm --restart=no`），镜像不设置 restart 策略。
FROM rust:1.87-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    CARGO_TARGET_DIR=/tmp/target

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openssh-server \
        python3 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 测试账号（密码认证）；只用于本机回环测试。
RUN useradd --create-home --shell /bin/bash termx \
    && echo 'termx:termx' | chpasswd

# 构建时生成 host key，避免每次启动等待；/run/sshd 由 entrypoint 兜底创建。
RUN mkdir -p /run/sshd && ssh-keygen -A

# 只放开密码认证；禁用 PAM，避免容器内 pam_loginuid 会话失败。
RUN printf '%s\n' \
        'PasswordAuthentication yes' \
        'KbdInteractiveAuthentication no' \
        'PermitRootLogin no' \
        'UsePAM no' \
        'AllowUsers termx' \
    > /etc/ssh/sshd_config.d/99-termx-test.conf

COPY entrypoint.sh /usr/local/bin/termx-sshd-entrypoint
RUN chmod 755 /usr/local/bin/termx-sshd-entrypoint

# T5 远端探针：模拟 termx-pwsh 的带外 shell 事件通道（POSIX shell + python3）。
COPY termx-pwsh /usr/local/bin/termx-pwsh
RUN chmod 755 /usr/local/bin/termx-pwsh

EXPOSE 2222
ENTRYPOINT ["/usr/local/bin/termx-sshd-entrypoint"]
