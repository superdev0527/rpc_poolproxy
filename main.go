package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"

	"github.com/rpcpool/pythd-fix-proxy/pkg/fix"
	"github.com/ybbus/jsonrpc/v2"
)

var addr = flag.String("addr", "localhost:8910", "http service address")

var cfgFileName = flag.String("cfg", "config.cfg", "Acceptor config file")

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill)

	flag.Parse()
	log.SetFlags(0)

	done := make(chan struct{})

	// Using FIX price feeds
	priceFeedCh, err := fix.Start(*cfgFileName, done)
	if err != nil {
		log.Panicf("Err start FIX %+v \n", err)
	}

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/"}
	log.Printf("connecting to %s", u.String())

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer conn.Close()

	priceAccountMap, err := sendListProductRq(conn)

	go func() {
		defer close(done)
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf("recv: %s", message)
		}
	}()

	go func() {
		if err != nil {
			log.Panicf("Can not have price account list %+v", err)
		}
		for priceFeed := range priceFeedCh {
			fmt.Printf("GOT Price feed: %+v", priceFeed)
			if enable, ok := fix.WhileListSymbol[priceFeed.Symbol]; !ok || !enable {
				continue
			}

			if priceAccount, ok := priceAccountMap[priceFeed.Symbol]; ok {
				fmt.Println(">>> Process to Pyth ", priceFeed.Symbol)
				err = sendUpdatePriceRq(conn, priceAccount, priceFeed.Price, priceFeed.Conf, "trading")
				if err != nil {
					log.Printf("Err %+vn", err)
				}
			}
		}
		fmt.Println("Pricechan ended")
	}()

	select {
	case <-done:
		return
	case <-interrupt:
		log.Println("interrupt")

		// Cleanly close the connection by sending a close message and then
		// waiting (with timeout) for the server to close the connection.
		err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			log.Println("write close:", err)
			return
		}
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		return
	}
}

func sendUpdatePriceRq(conn *websocket.Conn, accounts []PythPriceAccount, price decimal.Decimal, conf decimal.Decimal, status string) error {
	for _, account := range accounts {
		_price := price.Shift(-account.exponent)
		_conf := conf.Shift(-account.exponent)
		priceInt := _price.IntPart()
		confInt := _conf.IntPart()
		params := make(map[string]interface{}, 0)
		params["account"] = account.address
		params["price"] = priceInt
		params["conf"] = confInt
		params["status"] = status

		updatePriceRq := jsonrpc.NewRequest("update_price", params)
		b, err := json.Marshal(updatePriceRq)
		fmt.Printf("Send updatePriceRq : [%+s] \n", b)
		if err != nil {
			return fmt.Errorf("Err marsharl updatePriceRq %+v", err)
		}
		err = conn.WriteMessage(websocket.TextMessage, b)
		if err != nil {
			return fmt.Errorf("Err Write  update_price %+v", err)
		}
	}
	return nil
}

func sendSubscribePriceRq(conn *websocket.Conn, account string) error {
	params := make(map[string]interface{}, 0)
	params["account"] = account

	updatePriceRq := jsonrpc.NewRequest("subscribe_price", params)
	b, err := json.Marshal(updatePriceRq)
	if err != nil {
		return fmt.Errorf("Err marsharl updatePriceRq %+v", err)
	}

	err = conn.WriteMessage(websocket.TextMessage, b)
	if err != nil {
		return fmt.Errorf("Err Write subscribe_price %+v", err)
	}
	return nil
}

type PythPriceAccount struct {
	address  string
	exponent int32
}

func sendListProductRq(conn *websocket.Conn) (map[string][]PythPriceAccount, error) {
	productListRq := jsonrpc.NewRequest("get_product_list", nil)
	b, err := json.Marshal(productListRq)
	if err != nil {
		return nil, fmt.Errorf("Err marsharl getProductList %+v", err)
	}

	err = conn.WriteMessage(websocket.TextMessage, b)
	if err != nil {
		return nil, fmt.Errorf("Err Write get_product_list %+v", err)
	}

	_, data, err := conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("Err ReadMessage %+v", err)
	}
	var response RPCResponse
	err = json.Unmarshal(data, &response)
	if err != nil {
		return nil, fmt.Errorf("Err Unmarshal %+v", err)
	}

	mPriceAccounts := make(map[string][]PythPriceAccount)
	for _, result := range response.Result {
		if v, ok := result.AttrDict["generic_symbol"]; ok {
			priceAccounts := make([]PythPriceAccount, len(result.Price))
			for i, _v := range result.Price {
				priceAccounts[i] = PythPriceAccount{
					address:  _v.Account,
					exponent: _v.PriceExponent,
				}
			}
			mPriceAccounts[v] = priceAccounts
		}
	}
	return mPriceAccounts, nil
}

type RPCResponse struct {
	JSONRPC string            `json:"jsonrpc"`
	Result  []Result          `json:"result,omitempty"`
	Error   *jsonrpc.RPCError `json:"error,omitempty"`
	ID      int               `json:"id"`
}

type Result struct {
	Account  string            `json:"account,omitempty"`
	AttrDict map[string]string `json:"attr_dict,omitempty"`
	Price    []Price           `json:"price,omitempty"`
}

type Price struct {
	Account       string `json:"account,omitempty"`
	PriceExponent int32  `json:"price_exponent,omitempty"`
	PriceType     string `json:"price_type,omitempty"`
}
