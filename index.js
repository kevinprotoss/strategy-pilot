const path = require('path');
const moment = require('moment');
const { v4: uuidv4 } = require('uuid');
const grpc = require('@grpc/grpc-js');
const protoloader = require('@grpc/proto-loader');
const { Root } = require('protobufjs');
const Queue = require('bull');
const { Kafka } = require('kafkajs');

const rootPath = 'node_modules/jupiterapis/';

// 初始化Kafka
const kafka = new Kafka({
  clientId: 'strategy-pilot',
  brokers: ['kafka:9092']
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'strategy-pilot' });

// 载入jupiterapis数据定义
const root = new Root();
root.resolvePath = function(origin, target) {
  const relative = path.relative(rootPath, target);
  if (relative && !relative.startsWith('..') && !path.isAbsolute(relative)) {
    return path.join(origin, target);
  } else {
    return path.join(rootPath, target);
  }
};
root.loadSync([
  `${rootPath}jupiter/datastream/v1/future_data.proto`,
  `${rootPath}jupiter/strategy/common/v1/factor.proto`,
  `${rootPath}jupiter/trader/ctp/v1/instrument.proto`,
  `${rootPath}jupiter/trader/ctp/v1/order.proto`,
  `${rootPath}jupiter/trader/ctp/v1/order_action.proto`,
  `${rootPath}jupiter/trader/ctp/v1/trade.proto`,
  `${rootPath}jupiter/trader/ctp/v1/investor_position.proto`,
  `${rootPath}jupiter/trader/ctp/v1/investor_position_detail.proto`,
  `${rootPath}jupiter/trader/ctp/v1/trading_account.proto`,
]);
const Factor = root.lookupType('jupiter.strategy.common.v1.Factor');
const Order = root.lookupType('jupiter.trader.ctp.v1.Order');
const DirectionEnum = root.lookupType('jupiter.trader.ctp.v1.DirectionEnum');
const OffsetFlagEnum = root.lookupType('jupiter.trader.ctp.v1.OffsetFlagEnum');
const HedgeFlagEnum = root.lookupType('jupiter.trader.ctp.v1.HedgeFlagEnum');

// 创建GRPC客户端
const packageDefinition = protoloader.loadSync(`jupiter/trader/ctp/v1/query_service.proto`, {
  includeDirs: [rootPath],
  keepCase: true,
  enums: String,
});
const packageObject = grpc.loadPackageDefinition(packageDefinition);
const { QueryService } = packageObject.jupiter.trader.ctp.v1;
const queryService = new QueryService(`trader-ctp:6565`, grpc.credentials.createInsecure());

function queryDepthMarketData(instrumentID) {
  return new Promise((resolve, reject) => {
    queryService.queryDepthMarketData({ instrumentID }, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}

function queryInvestorPosition(instrumentID) {
  return new Promise((resolve, reject) => {
    const investorPositions = [];
    const call = queryService.queryInvestorPosition({
      brokerID: '9999',
      investorID: '012798',
      instrumentID,
      exchangeID: 'DCE'
    });
    call.on('data', investorPosition => investorPositions.push(investorPosition));
    call.on('end', () => resolve(investorPositions));
    call.on('error', err => reject(err));
  });
}

async function insertOrder(instrumentID, limitPrice, minVolume, direction, combOffsetFlag, orderPriceType) {
  const order = Order.create({
    UUID: uuidv4(),
    // orderRef: '10006',
    brokerID: '9999',
    investorID: '012798',
    instrumentID,
    orderPriceType,
    direction,
    combOffsetFlag,
    combHedgeFlag: HedgeFlagEnum.HedgeFlag.SPECULATION,
    limitPrice,
    volumeTotalOriginal: 1,
    timeCondition: Order.TimeCondition.GFD,
    volumeCondition: Order.VolumeCondition.AV,
    minVolume,
    contingentCondition: Order.ContingentCondition.IMMEDIATELY,
    forceCloseReason: Order.ForceCloseReason.NOT_FORCE_CLOSE,
  });
  console.log('order', order);
  await producer.send({
    topic: 'dev.trader.ctp.order.insert',
    messages: [
      { key: instrumentID, value: Order.encode(order).finish() },
    ],
  });
}

// 任务队列
const queue = new Queue('trader', 'redis://redis:6379');
queue.process('processSignal', async (job) => {
  const { factor } = job.data;
  const {
    name, 
    code: instrumentID, 
    date, 
    time, 
    value 
  } = factor;
  if (typeof value === 'undefined') {
    const err = new Error('Invalid factor values');
    console.error(err);
    throw err;
  }
  const [
    downLimit1 = 0,
    downLimit2 = 0,
    upLimit1 = 0,
    upLimit2 = 0,
    profit = 0
  ] = value.listValue.values.map(v => v.numberValue);
  console.log('instrumentID', instrumentID, 'profit', profit, 'limits', upLimit1, upLimit2, downLimit1, downLimit2);
  const jobOpts = { attempts: 5, timeout: 2000 };
  let investorPositions = await queryInvestorPosition(instrumentID);
  console.log(investorPositions);
  let today = moment();
  let yesterday = today.subtract(1, 'day');
  investorPositions = investorPositions.filter(ip => ip.tradingDay === yesterday.format('YYYYMMDD') && ip.position > 0);
  // 平昨
  for (let ydPosition of investorPositions) {
    console.log('平昨');
    if (ydPosition.posiDirection === 'LONG') {
      console.log('卖平');
      await queue.add('sellClose', { instrumentID, volume: ydPosition.position }, jobOpts);
    } else if (ydPosition.posiDirection === 'SHORT') {
      console.log('买平');
      await queue.add('buyClose', { instrumentID, volume: ydPosition.position }, jobOpts);
    }
  }
  console.log('开仓');
  const endTime = moment.min(moment().add(1, 'hour'), moment('15:00:00', 'HH:mm:ss'));
  const delay = endTime.diff(moment());
  if (profit > upLimit2) {
    // 买开
    await queue.add('buyOpen', { instrumentID, volume: 1 }, jobOpts);
    // 平仓或者锁掉
    jobOpts.delay = delay;
    await queue.add('sellClose', { instrumentID, volume: 1 }, jobOpts);
  } else if (profit < downLimit2) {
    // 卖开
    await queue.add('sellOpen', { instrumentID, volume: 1 }, jobOpts);
    // 平仓或者锁掉
    jobOpts.delay = delay;
    await queue.add('buyClose', { instrumentID, volume: 1 }, jobOpts);
  }
});

queue.process('buyOpen', async (job) => {
  console.log('buyOpen', job.data);
  const { instrumentID, volume } = job.data;
  const futureData = await queryDepthMarketData(instrumentID);
  const offerPriceLong = futureData.offerPrice[0];
  const offerPrice = offerPriceLong.toNumber() / 1000;
  await insertOrder(instrumentID, offerPrice, volume, DirectionEnum.Direction.BUY, OffsetFlagEnum.OffsetFlag.OPEN, Order.OrderPriceType.LIMIT_PRICE);
});

queue.process('buyClose', async (job) => {
  console.log('buyClose', job.data);
  const { instrumentID, volume } = job.data;
  const futureData = await queryDepthMarketData(instrumentID);
  const offerPriceLong = futureData.offerPrice[0];
  const offerPrice = offerPriceLong.toNumber() / 1000;
  await insertOrder(instrumentID, offerPrice, volume, DirectionEnum.Direction.BUY, OffsetFlagEnum.OffsetFlag.CLOSE, Order.OrderPriceType.LIMIT_PRICE);
});

queue.process('sellOpen', async (job) => {
  console.log('sellOpen', job.data);
  const { instrumentID, volume } = job.data;
  const futureData = await queryDepthMarketData(instrumentID);
  const bidPriceLong = futureData.bidPrice[0];
  const bidPrice = bidPriceLong.toNumber() / 1000;
  await insertOrder(instrumentID, bidPrice, volume, DirectionEnum.Direction.SELL, OffsetFlagEnum.OffsetFlag.OPEN, Order.OrderPriceType.LIMIT_PRICE);
});

queue.process('sellClose', async (job) => {
  console.log('sellClose', job.data);
  const { instrumentID, volume } = job.data;
  const futureData = await queryDepthMarketData(instrumentID);
  const bidPriceLong = futureData.bidPrice[0];
  const bidPrice = bidPriceLong.toNumber() / 1000;
  await insertOrder(instrumentID, bidPrice, volume, DirectionEnum.Direction.SELL, OffsetFlagEnum.OffsetFlag.CLOSE, Order.OrderPriceType.LIMIT_PRICE);
});

const run = async () => {
  // Producing
  await producer.connect();
  // Consuming
  await consumer.connect();
  await consumer.subscribe({ topic: 'factor.ctp.profit.5min' });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const factor = Factor.decode(message.value);
      await queue.add('processSignal', { 
        factor: Factor.toObject(factor, {
          longs: String,
          enums: String,
          bytes: String,
        }) 
      });
    },
  });
};

run().catch(console.error);
