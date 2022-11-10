package fix

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/quickfixgo/enum"
	"github.com/quickfixgo/field"
	"github.com/shopspring/decimal"

	_ "embed"

	fix42er "github.com/quickfixgo/fix42/executionreport"
	fix42lo "github.com/quickfixgo/fix42/logon"
	fix42mdir "github.com/quickfixgo/fix42/marketdataincrementalrefresh"
	fix42mdr "github.com/quickfixgo/fix42/marketdatarequest"
	fix42mdrr "github.com/quickfixgo/fix42/marketdatarequestreject"
	fix42mdsfr "github.com/quickfixgo/fix42/marketdatasnapshotfullrefresh"
	fix42sd "github.com/quickfixgo/fix42/securitydefinition"
	fix42sdr "github.com/quickfixgo/fix42/securitydefinitionrequest"
	"github.com/quickfixgo/quickfix"
)

//go:embed white_list.json
var whiteListData []byte

func init() {

	err := json.Unmarshal(whiteListData, &WhileListSymbol)
	if err != nil {
		panic("Can not Unmarshal whitelist")
	}

	fmt.Println("Whitelist: ", WhileListSymbol)
}

var WhileListSymbol map[string]bool

// var WhileListSymbol map[string]bool = map[string]bool{"ADAUSD": true, "BCHUSD": true, "DOTUSD": true}

type PriceFeed struct {
	Symbol string
	side   int
	Price  decimal.Decimal
	Conf   decimal.Decimal
}
type Application struct {
	mdReqID       int
	securityID    int
	sessionID     chan quickfix.SessionID
	priceChan     chan PriceFeed
	symbols       map[string]string
	priceFeedsMap map[string][]PriceFeed
	mu            sync.Mutex
	setting       *quickfix.SessionSettings
	*quickfix.MessageRouter
}

func (a *Application) GetPriceChan() <-chan PriceFeed {
	return a.priceChan
}

func (app *Application) genSecurityID() field.SecurityReqIDField {
	app.securityID++
	return field.NewSecurityReqID(strconv.Itoa(app.securityID))
}

func (app *Application) genMDID() field.MDReqIDField {
	app.mdReqID++
	return field.NewMDReqID(strconv.Itoa(app.mdReqID))
}

func newApp() *Application {
	app := Application{
		MessageRouter: quickfix.NewMessageRouter(),
		symbols:       make(map[string]string),
		sessionID:     make(chan quickfix.SessionID, 1),
		priceChan:     make(chan PriceFeed, 1000),
		priceFeedsMap: make(map[string][]PriceFeed),
	}
	app.AddRoute(fix42er.Route(app.OnFIX42ExecutionReport))
	app.AddRoute(fix42sd.Route(app.OnFIX42SecurityDefinition))
	app.AddRoute(fix42mdir.Route(app.OnFIX42MarketDataIncrementalRefresh))
	app.AddRoute(fix42mdrr.Route(app.OnFIX42MarketDataRequestReject))
	app.AddRoute(fix42mdsfr.Route(app.OnFIX42MarketDataSnapshotFullRefresh))
	app.AddRoute(fix42mdr.Route(app.OnFIX42MarketDataRequest))

	return &app
}

func (a *Application) OnFIX42MarketDataIncrementalRefresh(msg fix42mdir.MarketDataIncrementalRefresh, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	price_str, err := msg.GetString(quickfix.Tag(270))
	if err != nil {
		return err
	}
	side, err := msg.GetInt(quickfix.Tag(269))
	if err != nil {
		return err
	}

	// MDReqID, err := msg.GetInt(quickfix.Tag(262))
	// if err != nil {
	// 	return err
	// }

	symbol, err := msg.GetString(quickfix.Tag(55))
	if err != nil {
		return err
	}

	// price, _err := strconv.ParseFloat(price_str, 64)
	price, _err := decimal.NewFromString(price_str) //strconv.ParseFloat(price_str, 64)
	if _err != nil {
		return quickfix.IncorrectDataFormatForValue(quickfix.Tag(270))
	}

	a.priceFeedsMap[symbol] = append(a.priceFeedsMap[symbol], PriceFeed{
		Symbol: symbol,
		Price:  price,
		side:   side,
	})
	return nil
}

func (a *Application) OnFIX42MarketDataRequest(msg fix42mdr.MarketDataRequest, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	fmt.Printf("ON MarketDataRequest%+v \n", msg)
	return nil
}

func (a *Application) OnFIX42MarketDataRequestReject(msg fix42mdrr.MarketDataRequestReject, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	fmt.Printf("ON OnFIX42MarketDataRequestReject %+v \n", msg)
	return nil
}

func (a *Application) OnFIX42MarketDataSnapshotFullRefresh(msg fix42mdsfr.MarketDataSnapshotFullRefresh, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	fmt.Printf("ON OnFIX42MarketDataSnapshotFullRefresh \n %+v \n", msg)

	if price, err := msg.GetString(quickfix.Tag(270)); err != nil {
		fmt.Println("LOG PRICE >>> ", price)
	}
	return nil
}

func (a *Application) OnFIX42SecurityDefinition(msg fix42sd.SecurityDefinition, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	symbol, err := msg.GetSymbol()
	if err != nil {
		return err
	}

	{
		a.mu.Lock()
		a.symbols[symbol] = symbol
		defer a.mu.Unlock()
	}
	return nil
}

func (a *Application) OnFIX42ExecutionReport(msg fix42er.ExecutionReport, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	fmt.Printf("\n ===========OnFIX42ExecutionReport========== \n")
	fmt.Printf("%+v", msg)
	fmt.Printf("\n ===================== \n")
	return nil
}

//Notification of a session begin created.
func (a *Application) OnCreate(sessionID quickfix.SessionID) {
	fmt.Println("OnCreate")
}

func (a *Application) OnLogon(sessionID quickfix.SessionID) {
	fmt.Println("OnLogon")
	msg := fix42sdr.New(a.genSecurityID(), field.NewSecurityRequestType(enum.SecurityRequestType_SYMBOL))
	a.sessionID <- sessionID
	err := quickfix.SendToTarget(msg, sessionID)
	if err != nil {
		fmt.Printf("Error SendToTarget : %s,", err)
	} else {
		fmt.Printf("\nSend ok %+v \n", msg)
	}
}

//Notification of a session logging off or disconnecting.
func (a *Application) OnLogout(sessionID quickfix.SessionID) {
	fmt.Println("OnLogout")
}

//Notification of admin message being sent to target.
func (a *Application) ToAdmin(message *quickfix.Message, sessionID quickfix.SessionID) {
	password, err := a.setting.Setting("Password")
	if err != nil {
		panic(fmt.Sprintf("Miss SenderCompID %+v", err))
	}

	message.Header.SetString(quickfix.Tag(554), password)
}

//Notification of app message being sent to target.
func (a *Application) ToApp(message *quickfix.Message, sessionID quickfix.SessionID) error {
	return nil
}

//Notification of admin message being received from target.
func (a *Application) FromAdmin(message *quickfix.Message, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	return nil
}

//Notification of app message being received from target.
func (a *Application) FromApp(message *quickfix.Message, sessionID quickfix.SessionID) quickfix.MessageRejectError {
	return a.Route(message, sessionID)
}

type executor struct {
	*quickfix.MessageRouter
}

func Start(cfgFileName string, done <-chan struct{}) (<-chan PriceFeed, error) {
	cfg, err := os.Open(cfgFileName)
	if err != nil {
		return nil, fmt.Errorf("Error opening %v, %v\n", cfgFileName, err)
	}
	defer cfg.Close()
	stringData, readErr := ioutil.ReadAll(cfg)
	if readErr != nil {
		return nil, fmt.Errorf("Error reading cfg: %s,", readErr)
	}

	appSettings, err := quickfix.ParseSettings(bytes.NewReader(stringData))
	if err != nil {
		return nil, fmt.Errorf("Error reading cfg: %s,", err)
	}

	app := newApp()
	global := appSettings.SessionSettings()
	logFactory := quickfix.NewScreenLogFactory()
	for k, v := range global {
		if k.BeginString == quickfix.BeginStringFIX42 {
			app.setting = v
		}
	}

	quickApp, err := quickfix.NewInitiator(app, quickfix.NewMemoryStoreFactory(), appSettings, logFactory)
	if err != nil {
		return nil, fmt.Errorf("Unable to create Acceptor: %s\n", err)
	}

	err = quickApp.Start()
	if err != nil {
		return nil, fmt.Errorf("Unable to start Acceptor: %s\n", err)
	}

	app.subscribe()
	go func() {
		for {
			time.Sleep(time.Millisecond * 400)
			for symbol, prices := range app.priceFeedsMap {
				var bidTotal, askTotal decimal.Decimal
				var bidCount, askCount int64
				for _, price := range prices {
					if price.side == 0 {
						bidTotal = bidTotal.Add(price.Price)
						bidCount += 1
					} else if price.side == 1 {
						askCount += 1
						askTotal = askTotal.Add(price.Price)
					}
				}

				if askCount == 0 || bidCount == 0 {
					fmt.Println("Not enoughs side")
					continue
				}
				bid := bidTotal.Div(decimal.NewFromInt(bidCount))
				ask := askTotal.Div(decimal.NewFromInt(askCount))
				conf := bid.Sub(ask).Abs().Div(decimal.NewFromInt(2))
				app.priceChan <- PriceFeed{
					Symbol: symbol,
					Price:  bid,
					Conf:   conf,
				}
				app.priceChan <- PriceFeed{
					Symbol: symbol,
					Price:  ask,
					Conf:   conf,
				}
				delete(app.priceFeedsMap, symbol)
			}
		}
	}()
	go func() {
		<-done
		fmt.Println("Stopped quickapp")
		fmt.Println("Close Pricechan")
		close(app.priceChan)
		quickApp.Stop()
	}()

	return app.GetPriceChan(), nil
}

func (app *Application) subscribe() {
	sessionID := <-app.sessionID

	fmt.Printf("\n WhileListSymbol [%+v] \n", WhileListSymbol)
	time.Sleep(5 * time.Second)
	for v := range WhileListSymbol {
		if symbol, ok := app.symbols[v]; ok {
			msg := app.makeFix42MarketDataRequest(symbol)
			err := quickfix.SendToTarget(msg, sessionID)
			if err != nil {
				fmt.Printf(">>>>> Error SendToTarget : %v,", err)
			}
		} else {
			fmt.Printf("Our symbol not supported by jump: %s \n", v)
		}
	}
}

func (app *Application) makeFix42MarketDataRequest(symbol string) *quickfix.Message {
	fmt.Printf("%+v", app.setting)
	sender, err := app.setting.Setting("SenderCompID")
	if err != nil {
		panic(fmt.Sprintf("Miss SenderCompID %+v", err))
	}
	target, err := app.setting.Setting("TargetCompID")
	if err != nil {
		panic(fmt.Sprintf("Miss SenderCompID %+v", err))
	}

	clientID, err := app.setting.Setting("ClientID")
	if err != nil {
		panic(fmt.Sprintf("Miss SenderCompID %+v", err))
	}

	mdID := app.genMDID()

	request := fix42mdr.New(mdID,
		field.NewSubscriptionRequestType(enum.SubscriptionRequestType_SNAPSHOT_PLUS_UPDATES),
		field.NewMarketDepth(0),
	)
	request.SetSenderCompID(sender)
	request.SetTargetCompID(target)
	request.SetString(quickfix.Tag(109), clientID)

	entryTypes := fix42mdr.NewNoMDEntryTypesRepeatingGroup()
	entryTypes.Add().SetMDEntryType(enum.MDEntryType_BID)
	entryTypes.Add().SetMDEntryType(enum.MDEntryType_OFFER)
	request.SetNoMDEntryTypes(entryTypes)

	relatedSym := fix42mdr.NewNoRelatedSymRepeatingGroup()
	relatedSym.Add().SetSymbol(symbol)
	request.SetNoRelatedSym(relatedSym)

	request.SetString(quickfix.Tag(5000), "0")

	request.SetMDUpdateType(enum.MDUpdateType_INCREMENTAL_REFRESH)

	return request.ToMessage()
}

func (app *Application) makeFix42Logon() *quickfix.Message {
	password, err := app.setting.Setting("Password")
	if err != nil {
		panic(fmt.Sprintf("Miss SenderCompID %+v", err))
	}

	request := fix42lo.New(field.NewEncryptMethod(enum.EncryptMethod_NONE_OTHER), field.NewHeartBtInt(5))
	request.Header.SetString(quickfix.Tag(554), password)

	return request.ToMessage()
}
