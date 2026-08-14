class Remi < Formula
  desc "Unified coding-agent session memory"
  homepage "https://github.com/lsj5031/Remi"
  license "MIT OR Apache-2.0"
  version "0.2.0"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/lsj5031/Remi/releases/download/v0.2.0/remi-macos-arm64-simple.tar.gz"
      sha256 "de3920c4f11b825477c1239343bcebe367951d9ad6b660654187f9c32c0c94ae"
    else
      odie "macOS x86_64 release asset is not published yet."
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      odie "Linux arm64 artifact is not published yet"
    else
      url "https://github.com/lsj5031/Remi/releases/download/v0.2.0/remi-linux-x64-simple.tar.gz"
      sha256 "865fea09a7982c2928599ca92db0ca46f095c403bc27f6c69fa1d6fc74a11a6d"
    end
  end

  def install
    bin.install "remi"
  end

  test do
    assert_match "Unified coding-agent session memory", shell_output("#{bin}/remi --help")
  end
end
