package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"text/template"

	"github.com/docker/docker/api/types"
	"github.com/spf13/pflag"
	"github.com/togettoyou/hub-mirror/pkg"
)

var (
	content    = pflag.StringP("content", "", "", "原始镜像，格式为：{ \"platform\": \"\", \"hub-mirror\": [] }")
	maxContent = pflag.IntP("maxContent", "", 11, "每批次并发处理的最大镜像数量")
	repository = pflag.StringP("repository", "", "", "推送仓库地址，为空默认为 hub.docker.com")
	username   = pflag.StringP("username", "", "", "仓库用户名")
	password   = pflag.StringP("password", "", "", "仓库密码")
	outputPath = pflag.StringP("outputPath", "", "output.md", "结果输出路径")
)

func main() {
	pflag.Parse()

	fmt.Println("验证原始镜像内容")
	var hubMirrors struct {
		Content  []string `json:"hub-mirror"`
		Platform string   `json:"platform"`
	}

	err := json.Unmarshal([]byte(*content), &hubMirrors)
	if err != nil {
		panic(err)
	}

	// 过滤掉空字符串
	mirrors := make([]string, 0, len(hubMirrors.Content))
	for _, m := range hubMirrors.Content {
		if m != "" {
			mirrors = append(mirrors, m)
		}
	}

	fmt.Printf("共 %d 个镜像待处理，平台: %s\n", len(mirrors), hubMirrors.Platform)

	fmt.Println("初始化 Docker 客户端")
	cli, err := pkg.NewCli(context.Background(), *repository, *username, *password, os.Stdout)
	if err != nil {
		panic(err)
	}

	outputs := make([]*pkg.Output, 0)
	mu := sync.Mutex{}

	// 计算总批次数
	totalBatches := (len(mirrors) + *maxContent - 1) / *maxContent

	// 分批处理
	for i := 0; i < len(mirrors); i += *maxContent {
		end := i + *maxContent
		if end > len(mirrors) {
			end = len(mirrors)
		}
		batch := mirrors[i:end]
		batchNum := i / *maxContent
		fmt.Printf("\n--- 处理第 %d/%d 批，本批 %d 个镜像 ---\n", batchNum+1, totalBatches, len(batch))

		// 用于记录本批次成功处理的镜像信息（包括源镜像和目标镜像名），以便后续清理
		type batchResult struct {
			source string
			target string
		}
		batchResults := make([]batchResult, 0, len(batch))
		var batchMu sync.Mutex

		wg := sync.WaitGroup{}
		for _, source := range batch {
			source := source
			wg.Add(1)
			go func() {
				defer wg.Done()

				fmt.Printf("[%s] 开始转换\n", source)
				output, err := cli.PullTagPushImage(context.Background(), source, hubMirrors.Platform)
				if err != nil {
					fmt.Printf("[%s] 转换失败: %v\n", source, err)
					return
				}

				mu.Lock()
				outputs = append(outputs, output)
				mu.Unlock()

				// 记录本批次成功的结果，用于后续清理
				batchMu.Lock()
				batchResults = append(batchResults, batchResult{source: output.Source, target: output.Target})
				batchMu.Unlock()

				fmt.Printf("[%s] 转换成功 -> %s\n", source, output.Target)
			}()
		}
		wg.Wait()
		fmt.Printf("--- 第 %d 批完成 ---\n", batchNum+1)

		// 清理本批次拉取和生成的镜像，释放磁盘空间
		fmt.Printf("正在清理第 %d 批的本地镜像...\n", batchNum+1)
		for _, res := range batchResults {
			// 删除源镜像
			if _, err := cli.ImageRemove(context.Background(), res.source, types.ImageRemoveOptions{Force: true}); err != nil {
				fmt.Printf("警告: 删除源镜像 %s 失败: %v\n", res.source, err)
			}
			// 删除目标镜像（标签）
			if _, err := cli.ImageRemove(context.Background(), res.target, types.ImageRemoveOptions{Force: true}); err != nil {
				fmt.Printf("警告: 删除目标镜像 %s 失败: %v\n", res.target, err)
			}
		}
		fmt.Printf("第 %d 批本地镜像清理完成\n", batchNum+1)
	}

	if len(outputs) == 0 {
		panic("没有转换成功的镜像")
	}

	// 渲染输出模板
	tmpl, err := template.ParseFiles("output.tmpl")
	if err != nil {
		panic(err)
	}
	outputFile, err := os.Create(*outputPath)
	if err != nil {
		panic(err)
	}
	defer outputFile.Close()

	err = tmpl.Execute(outputFile, map[string]interface{}{
		"Outputs":    outputs,
		"Repository": *repository,
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("\n全部处理完成，成功 %d 个镜像，结果写入 %s\n", len(outputs), *outputPath)
}