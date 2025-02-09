package main

import (
	"log"
	"net/http"
	"sync"
	"github.com/gorilla/websocket"
)

// WebSocket のアップグレーダー設定（どのオリジンからも接続を許可）
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Client は接続してきたクライアントの情報を保持します。
type Client struct {
	conn    *websocket.Conn
	partner *Client
}

// 待機中のクライアントを管理するためのグローバル変数と排他制御用のミューテックス
var (
	waitingClient *Client
	waitingMutex  sync.Mutex
)

// handleWebSocket は /ws エンドポイントに対するリクエストを処理し、WebSocket 接続を確立します。
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// HTTP コネクションを WebSocket にアップグレード
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	client := &Client{conn: conn}
	log.Println("新しいクライアントが接続しました")

	// 待機中のクライアントとのペアリングを試みる
	waitingMutex.Lock()
	if waitingClient == nil {
		// 待機中のクライアントがいなければ、このクライアントを待機状態に設定
		waitingClient = client
		waitingMutex.Unlock()

		// クライアントに待機中である旨を通知
		client.conn.WriteMessage(websocket.TextMessage, []byte("Waiting for a partner..."))

		// ペアが成立するまで接続を維持（ここでは単にブロックしています）
		select {}
	} else {
		// 待機中のクライアントが存在する場合、ペアリングを行う
		partner := waitingClient
		waitingClient = nil
		waitingMutex.Unlock()

		// 相互にパートナーを設定
		client.partner = partner
		partner.partner = client

		// 両クライアントにペア成立を通知
		client.conn.WriteMessage(websocket.TextMessage, []byte("Partner found! Say hi."))
		partner.conn.WriteMessage(websocket.TextMessage, []byte("Partner found! Say hi."))

		// 両者間でメッセージの中継処理を開始
		go relayMessages(client)
		go relayMessages(partner)
	}
}

// relayMessages は、接続されたクライアントからのメッセージをそのパートナーに転送します。
func relayMessages(client *Client) {
	defer client.conn.Close()
	for {
		// クライアントからメッセージを受信
		msgType, msg, err := client.conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			// エラー時はパートナーに切断通知を行い、パートナーの接続も閉じる
			if client.partner != nil {
				client.partner.conn.WriteMessage(websocket.TextMessage, []byte("Your partner disconnected."))
				client.partner.conn.Close()
			}
			break
		}
		// パートナーが存在すれば、メッセージを転送する
		if client.partner != nil {
			err := client.partner.conn.WriteMessage(msgType, msg)
			if err != nil {
				log.Println("Write error:", err)
				break
			}
		} else {
			// パートナーがいない場合は、待機中である旨を返す
			client.conn.WriteMessage(websocket.TextMessage, []byte("Still waiting for a partner..."))
		}
	}
}

func main() {
	// /ws エンドポイントに対してハンドラを設定
	http.HandleFunc("/ws", handleWebSocket)
	log.Println("WebSocket サーバーを :8080 で起動します")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}
