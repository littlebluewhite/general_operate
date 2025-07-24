#!/bin/bash

# General Operate Test Runner Script
# 提供完整的測試執行和覆蓋率報告功能

set -e  # 遇到錯誤時退出

# 顏色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 函數：打印彩色輸出
print_color() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# 函數：顯示幫助信息
show_help() {
    echo "General Operate Test Runner"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help              顯示此幫助信息"
    echo "  -f, --fast              快速測試（不生成覆蓋率報告）"
    echo "  -c, --coverage          運行測試並生成覆蓋率報告"
    echo "  -r, --report-only       僅生成覆蓋率報告（不運行測試）"
    echo "  -s, --specific FILE     運行特定測試文件"
    echo "  -v, --verbose           詳細輸出"
    echo "  --html                  生成 HTML 覆蓋率報告"
    echo "  --xml                   生成 XML 覆蓋率報告"
    echo "  --json                  生成 JSON 覆蓋率報告"
    echo ""
    echo "Examples:"
    echo "  $0 -c                   運行所有測試並生成覆蓋率報告"
    echo "  $0 -f                   快速運行所有測試"
    echo "  $0 -s test_utils.py     運行特定測試文件"
    echo "  $0 -c --html            運行測試並生成 HTML 覆蓋率報告"
}

# 函數：檢查項目結構
check_project_structure() {
    print_color $BLUE "🔍 檢查項目結構..."
    
    if [ ! -f "pyproject.toml" ]; then
        print_color $RED "❌ 錯誤：未找到 pyproject.toml 文件"
        exit 1
    fi
    
    if [ ! -d "general_operate" ]; then
        print_color $RED "❌ 錯誤：未找到 general_operate 源碼目錄"
        exit 1
    fi
    
    if [ ! -d "tests" ]; then
        print_color $RED "❌ 錯誤：未找到 tests 測試目錄"
        exit 1
    fi
    
    print_color $GREEN "✅ 項目結構檢查通過"
}

# 函數：運行測試
run_tests() {
    local test_file="$1"
    local verbose="$2"
    local coverage="$3"
    
    print_color $BLUE "🧪 開始運行測試..."
    
    local cmd="uv run pytest"
    
    # 添加特定測試文件
    if [ -n "$test_file" ]; then
        if [ -f "tests/$test_file" ]; then
            cmd="$cmd tests/$test_file"
        elif [ -f "$test_file" ]; then
            cmd="$cmd $test_file"
        else
            print_color $RED "❌ 測試文件不存在: $test_file"
            exit 1
        fi
    fi
    
    # 添加詳細輸出
    if [ "$verbose" = true ]; then
        cmd="$cmd -v"
    fi
    
    # 添加覆蓋率選項
    if [ "$coverage" = true ]; then
        cmd="$cmd --cov=general_operate --cov-report=term"
        
        # 根據參數添加不同格式的報告
        if [ "$GENERATE_HTML" = true ]; then
            cmd="$cmd --cov-report=html"
        fi
        
        if [ "$GENERATE_XML" = true ]; then
            cmd="$cmd --cov-report=xml"
        fi
        
        if [ "$GENERATE_JSON" = true ]; then
            cmd="$cmd --cov-report=json"
        fi
    fi
    
    print_color $YELLOW "執行命令: $cmd"
    echo ""
    
    if eval $cmd; then
        print_color $GREEN "✅ 測試執行成功"
        return 0
    else
        print_color $RED "❌ 測試執行失敗"
        return 1
    fi
}

# 函數：生成覆蓋率報告摘要
generate_coverage_summary() {
    if [ -f ".coverage" ]; then
        print_color $BLUE "📊 生成覆蓋率摘要..."
        
        echo ""
        print_color $YELLOW "=== 覆蓋率報告摘要 ==="
        
        # 使用 coverage 命令生成報告
        if command -v coverage >/dev/null 2>&1; then
            uv run coverage report --show-missing
        else
            print_color $YELLOW "⚠️  coverage 工具未安裝，無法生成詳細摘要"
        fi
        
        echo ""
        
        # 顯示生成的報告文件
        if [ -d "htmlcov" ]; then
            print_color $GREEN "✅ HTML 覆蓋率報告已生成: htmlcov/index.html"
        fi
        
        if [ -f "coverage.xml" ]; then
            print_color $GREEN "✅ XML 覆蓋率報告已生成: coverage.xml"
        fi
        
        if [ -f "coverage.json" ]; then
            print_color $GREEN "✅ JSON 覆蓋率報告已生成: coverage.json"
        fi
    else
        print_color $YELLOW "⚠️  未找到覆蓋率數據文件"
    fi
}

# 函數：清理舊的報告文件
cleanup_old_reports() {
    print_color $BLUE "🧹 清理舊的報告文件..."
    
    if [ -d "htmlcov" ]; then
        rm -rf htmlcov
        print_color $GREEN "✅ 已清理舊的 HTML 報告"
    fi
    
    if [ -f "coverage.xml" ]; then
        rm -f coverage.xml
        print_color $GREEN "✅ 已清理舊的 XML 報告"
    fi
    
    if [ -f "coverage.json" ]; then
        rm -f coverage.json
        print_color $GREEN "✅ 已清理舊的 JSON 報告"
    fi
    
    if [ -f ".coverage" ]; then
        rm -f .coverage
        print_color $GREEN "✅ 已清理舊的覆蓋率數據"
    fi
}

# 默認參數
FAST_MODE=false
COVERAGE_MODE=false
REPORT_ONLY=false
VERBOSE=false
SPECIFIC_FILE=""
GENERATE_HTML=false
GENERATE_XML=false
GENERATE_JSON=false

# 解析命令行參數
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -f|--fast)
            FAST_MODE=true
            shift
            ;;
        -c|--coverage)
            COVERAGE_MODE=true
            shift
            ;;
        -r|--report-only)
            REPORT_ONLY=true
            shift
            ;;
        -s|--specific)
            SPECIFIC_FILE="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --html)
            GENERATE_HTML=true
            shift
            ;;
        --xml)
            GENERATE_XML=true
            shift
            ;;
        --json)
            GENERATE_JSON=true
            shift
            ;;
        *)
            print_color $RED "❌ 未知選項: $1"
            show_help
            exit 1
            ;;
    esac
done

# 主執行邏輯
main() {
    print_color $BLUE "🚀 General Operate Test Runner 開始執行"
    echo ""
    
    # 檢查項目結構
    check_project_structure
    echo ""
    
    # 如果只生成報告
    if [ "$REPORT_ONLY" = true ]; then
        generate_coverage_summary
        exit 0
    fi
    
    # 清理舊報告（覆蓋率模式）
    if [ "$COVERAGE_MODE" = true ]; then
        cleanup_old_reports
        echo ""
    fi
    
    # 決定是否啟用覆蓋率
    local enable_coverage=false
    if [ "$COVERAGE_MODE" = true ] && [ "$FAST_MODE" = false ]; then
        enable_coverage=true
    fi
    
    # 運行測試
    if run_tests "$SPECIFIC_FILE" "$VERBOSE" "$enable_coverage"; then
        echo ""
        
        # 生成覆蓋率摘要
        if [ "$enable_coverage" = true ]; then
            generate_coverage_summary
        fi
        
        echo ""
        print_color $GREEN "🎉 測試運行完成！"
        
        # 顯示下一步建議
        if [ "$enable_coverage" = true ]; then
            echo ""
            print_color $YELLOW "💡 建議："
            echo "   - 查看 HTML 報告: open htmlcov/index.html"
            echo "   - 提高測試覆蓋率以確保代碼質量"
        fi
        
        exit 0
    else
        echo ""
        print_color $RED "💥 測試運行失敗！"
        print_color $YELLOW "請檢查測試輸出並修復失敗的測試"
        exit 1
    fi
}

# 運行主函數
main