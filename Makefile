# General Operate Makefile
# 提供常用的開發和測試命令

.PHONY: help test test-fast test-coverage test-html clean install dev lint format type-check docs

# 默認目標
help:
	@echo "General Operate 開發工具"
	@echo ""
	@echo "可用命令："
	@echo "  make install       安裝項目依賴"
	@echo "  make dev           安裝開發依賴"
	@echo "  make test          運行所有測試"
	@echo "  make test-fast     快速運行測試（不生成覆蓋率）"
	@echo "  make test-coverage 運行測試並生成覆蓋率報告"
	@echo "  make test-html     運行測試並生成 HTML 覆蓋率報告"
	@echo "  make test-file     運行特定測試文件 (使用 FILE=filename)"
	@echo "  make lint          代碼檢查"
	@echo "  make format        代碼格式化"
	@echo "  make type-check    類型檢查"
	@echo "  make clean         清理生成的文件"
	@echo "  make docs          生成文檔"
	@echo ""
	@echo "示例："
	@echo "  make test-file FILE=test_utils.py"
	@echo "  make test-coverage"

# 安裝項目依賴
install:
	@echo "📦 安裝項目依賴..."
	uv sync

# 安裝開發依賴
dev:
	@echo "🔧 安裝開發依賴..."
	uv sync --dev

# 運行所有測試
test:
	@echo "🧪 運行所有測試..."
	./scripts/run_tests.sh -v

# 快速測試（無覆蓋率）
test-fast:
	@echo "⚡ 快速運行測試..."
	./scripts/run_tests.sh -f -v

# 測試覆蓋率
test-coverage:
	@echo "📊 運行測試並生成覆蓋率報告..."
	./scripts/run_tests.sh -c -v

# HTML 覆蓋率報告
test-html:
	@echo "🌐 生成 HTML 覆蓋率報告..."
	./scripts/run_tests.sh -c -v --html
	@echo "✅ HTML 報告已生成，運行以下命令查看："
	@echo "   open htmlcov/index.html"

# 運行特定測試文件
test-file:
ifndef FILE
	@echo "❌ 請指定測試文件: make test-file FILE=test_utils.py"
	@exit 1
endif
	@echo "🎯 運行測試文件: $(FILE)"
	./scripts/run_tests.sh -s $(FILE) -v

# 代碼檢查
lint:
	@echo "🔍 代碼檢查..."
	uv run ruff check general_operate tests
	@echo "✅ 代碼檢查完成"

# 代碼格式化
format:
	@echo "🎨 代碼格式化..."
	uv run ruff format general_operate tests
	uv run ruff check --fix general_operate tests
	@echo "✅ 代碼格式化完成"

# 類型檢查
type-check:
	@echo "🔍 類型檢查..."
	uv run mypy general_operate --ignore-missing-imports
	@echo "✅ 類型檢查完成"

# 清理生成的文件
clean:
	@echo "🧹 清理生成的文件..."
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf coverage.json
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "✅ 清理完成"

# 生成文檔
docs:
	@echo "📚 生成文檔..."
	@echo "⚠️  文檔生成功能待實現"

# 完整的 CI 檢查
ci-check: clean lint type-check test-coverage
	@echo "🎉 CI 檢查全部通過！"

# 開發環境設置
dev-setup: dev
	@echo "🔧 設置開發環境..."
	@echo "✅ 開發環境設置完成"
	@echo ""
	@echo "💡 常用命令："
	@echo "   make test-fast     - 快速測試"
	@echo "   make test-html     - 測試 + HTML 覆蓋率報告"
	@echo "   make lint          - 代碼檢查"
	@echo "   make format        - 代碼格式化"

# 發布前檢查
pre-release: clean format lint type-check test-coverage
	@echo "🚀 發布前檢查完成！"

# 查看測試統計
test-stats:
	@echo "📈 測試統計信息..."
	@find tests -name "test_*.py" | wc -l | xargs echo "測試文件數量:"
	@find tests -name "test_*.py" -exec grep -h "def test_" {} \; | wc -l | xargs echo "測試函數數量:"
	@echo ""
	@echo "📊 最近的覆蓋率報告："
	@if [ -f ".coverage" ]; then \
		uv run coverage report --format=total; \
	else \
		echo "無覆蓋率數據，請先運行 make test-coverage"; \
	fi