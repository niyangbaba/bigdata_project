/**
 * Created by Administrator on 2018/12/19.
 */

(function () {
    //读写cookie的数据
    var cookieUtils = {
        //写cookie的数据
        set : function (cookieName, cookieValue) {
            //对cookie进行加密拼接
            var cookieText = encodeURIComponent(cookieName) + "=" + encodeURIComponent(cookieValue);
            //设置cookie的过期时间
            //获取当前时间
            var date = new Date();
            //获取当前时间的时间戳
            var currentTime = date.getTime();
            //10年时间的时间戳
            var tenYearsCurrentTimes = 10 * 365 * 24 * 60 * 60 *1000;
            //10年后的时间戳 并赋值给date
            var cookieOutCurrentTime = currentTime + tenYearsCurrentTimes;
            date.setTime(cookieOutCurrentTime);
            //cookie的过期时间   十年后的UTC时间
            cookieText += ";expires=" +  date.toUTCString();
            document.cookie = cookieText;
        },

        //从浏览器中读cookie的数据
        get : function (cookieName) {
            //读cookie document.cookie   UUID=19981114;a=11;b=14
            cookieName = encodeURIComponent(cookieName) + "=";
            var cookieValue = null;
            //indexOf 如果找到了返回字符串的开始位置，找不到返回-1
            var cookieStart = document.cookie.indexOf(cookieName);
            //找到该cookie的id
            if (cookieStart != -1) {
                //获取该cookie键值对后 ; 的下标
                var cookieEnd = document.cookie.indexOf(";", cookieStart);
                //如果没有获取到 则cookieEnd 的下标为 document.cookie.length
                if (cookieEnd == -1) {
                    cookieEnd = document.cookie.length
                }
                //subString 包左不包右
                //截取出value 并解码 赋值
                cookieValue =decodeURIComponent(document.cookie.substring(cookieStart + cookieName.length, cookieEnd));
            }
            return cookieValue;
        }
    };

    var tracker = {

        //客户端信息
        clientConfig : {
            //日志服务器地址
            logServerUrl : "http://h21/log.gif" ,
            //sessionOutTime 2min
            sessionOutTime : 2*60*1000,
            //js sdk 版本
            version : "1.0"
        },

        /**
         * 定义需要采集的事件，需要采集哪些事件，根据具体业务来
         * 总体来说事件分为两类：
         * 1，手动触发事件：搜索事件，加入购物车事件，登录事件
         * 2，自动触发事件：首次访问事件,pv事件。。
         */
        eventName : {
            //首次访问事件
            launchEvent : "e_l",
            //页面浏览事件
            pageViewEvent: "e_pv",
            //搜索事件
            searchEvent: "e_s",
            //加入购物车事件
            addCartEvent: "e_ad"
        },

        /**
         * 定义需要采集的字段名称
         */
        columns : {
            eventName: "en",//事件名称
            version: "ver",//日志版本
            platform: "pl",//平台 ios Android
            sdk: "sdk",//sdk
            uuid: "uuid",//用户唯一标识
            sessionId: "sid",//会话id
            resolution: "b_rst",//浏览器分辨率
            userAgent: "b_usa",//浏览器代理信息
            language: "l",//语言
            clientTime: "ct",//客户端时间
            currentUrl: "url",//当前页面的url
            referrerUrl: "ref",//来源url，上一个页面的url
            title: "tt",//网页标题
            keyword: "kw",//搜索关键字
            goodsId: "gid"//商品id
        },

        /**
         * 需要保存到cookie中的字段名称
         */
        keys : {
            //用户标识
            uuid : "uuid",
            //会话标识
            sid : "sid",
            //用户最近一次访问时间
            preVisitTime : "preVisitTime"
        },

        /**
         * 设置uuid等信息到cookie中
         * 调用cookieUtils的set方法和get方法
         */
        setUuid : function (uuid) {
            cookieUtils.set(this.keys.uuid,uuid)
        },
        getUuid : function () {
            return  cookieUtils.get(this.keys.uuid)
        },

        setSid : function (sid) {
            cookieUtils.set(this.keys.sid,sid)
        },
        getSid :function () {
            return  cookieUtils.get(this.keys.sid)
        },


        /**
         * 会话开始
         */
        sessionStart : function () {
            //判断会话是否存在
            //null "" ===>false
            //其他情况都为 true

            if (this.getSid()){//会话存在
                //判断会话是否过期
                if(this.isSessionTimeOut){
                    this.createNewSession();
                }else {
                    //会话没有过期  更新会话时间
                    this.updatePreVisitTime();
                }
            }else {
                //会话不存在
                //创建一个新的会话
                this.createNewSession();
            }
            //触发pageView 事件
            this.pageViewEvent();
        },

        /**
         * 创建一个新的会话
         */
        createNewSession : function () {
            //生成会话ID
            var sid = this.generateUUID();
            //将sid 保存至cookie中
            this.setSid(sid);
            //判断用户是否是首次访问 （从cookie中读取uuid，能读取到非首次访问，否则首次访问）
            if(!this.getUuid()){ //首次访问
                //生成uuid保存到cookie中
                this.setUuid(this.generateUUID());
                //触发首次访问事件
                this.launchEvent();
            }
        },

        /**
         * 首次访问事件
         */
        launchEvent :function () {
            var data = {};
            //设置事件名称
            data[this.columns.eventName] = this.eventName.launchEvent;
            //设置公共字段
            this.setCommonColumns(data);
            //将数据发送到日志服务器上
            this.sendDataToLogServer(data);
        },

        /**
         * 用户浏览页面事件
         */
        pageViewEvent : function () {
            if(this.callBack()){
                var data = {};
                //设置事件名称
                data[this.columns.eventName] = this.eventName.pageViewEvent;
                //设置公共字段
                this.setCommonColumns(data);
                //当前页面url
                data[this.columns.currentUrl] = window.location.href;
                //获取来源页面url  及上个界面的url
                data[this.columns.referrerUrl] = document.referrer;
                //获取当前页面标题
                data[this.columns.title] = document.title;
                //发送数据
                this.sendDataToLogServer(data);
            }
        },

        /**
         * 搜索事件
         */
        searchEvent :function (keyWord) {
            if (this.callBack()){
                var data = {};
                //设置事件名称
                data[this.columns.eventName] = this.eventName.searchEvent;
                //设置公共字段
                this.setCommonColumns(data);
                //设置搜索信息
                data[this.columns.goodsId] = keyWord;
                //发送数据
                this.sendDataToLogServer(data);
            }
        },

        /**
         * 加入购物车事件
         * @param gid
         */
        addCartEvent :function (gid) {
            if (this.callBack()){
                var data = {};
                //设置事件名称
                data[this.columns.eventName] = this.eventName.addCartEvent;
                //设置公共字段
                this.setCommonColumns(data);
                //设置物品名称
                data[this.columns.goodsId] = gid;
                //发送数据
                this.sendDataToLogServer(data);
            }
        },

        /**
         * 会话过期创建新的会话
         * @returns {boolean}
         */
        callBack : function () {
            if (this.isSessionTimeOut()){
                this.createNewSession();
            }
            return true;
        },

        /**
         * 判断会话是否过期
         */
        isSessionTimeOut: function () {
            //获取当前时间
            var currentTime = new Date().getTime();
            //获取最近一次访问时间
            var preVisitTime = cookieUtils.get(this.keys.preVisitTime);
            //判断会话是否过期
            return currentTime - preVisitTime > this.clientConfig.sessionOutTime;
        },

        /**
         * 数据发送到日志服务器上
         * @param data
         */
        sendDataToLogServer:function (data) {

            //对data 对象 key 和value进行编码，并整理成字符串
            var paramsText = this.encodeData(data);
            var image = new Image(1,1);
            image.src = this.clientConfig.logServerUrl + "?" + paramsText;
            // 更新用户最近访问时间
            this.updatePreVisitTime();
        },

        /**
         *更新用户最近访问时间
         */
        updatePreVisitTime :function () {
            cookieUtils.set(this.keys.preVisitTime,new Date().getTime());
        },

        /**
         * 对data 对象 key 和value进行编码，并整理成字符串
         * @param data
         */
        encodeData : function (data) {
            var paramsText = "";
            for( key in data){
                if(key && data[key]){ //判断key和value都不为空的情况
                    paramsText += encodeURIComponent(key) + "=" + encodeURIComponent(data[key]) + "&";
                }
            }
            if (paramsText){
                paramsText =  paramsText.substring(0,paramsText.length-1);
            }
            return paramsText;
        },


        /**
         * 设置公共字段
         * @param data
         */
        setCommonColumns :function (data) {
            //sdk版本
            data[this.columns.sdk] = this.clientConfig.version;
            //sdk类型
            data[this.columns.version] = "js";
            //用户唯一标识
            data[this.columns.uuid] = this.getUuid();
            //用户会话id
            data[this.columns.sessionId] = this.getSid();
            //浏览器分辨率
            data[this.columns.resolution] = window.screen.height + "*" + window.screen.width;
            //浏览器代理信息
            var userAgent = window.navigator.userAgent.toLowerCase();
            data[this.columns.userAgent] = userAgent;
            if (userAgent.indexOf("ios") != -1){
                data[this.columns.platform] = "ios";
            }else if(userAgent.indexOf("android") != -1){
                data[this.columns.platform] = "android";
            }else if (userAgent.indexOf("windows") != -1 || userAgent.indexOf("linux") != -1){
                data[this.columns.platform] = "pc";
            }else {
                data[this.columns.platform] = "other";
            }
            //用户所使用的语言
            data[this.columns.language] = window.navigator.language;
            //客户端时间
            data[this.columns.clientTime] = new Date().getTime();
        },


        /**
         * 生成唯一标识的方法
         */
        generateUUID: function () {
            var d = new Date().getTime();
            var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
                var r = (d + Math.random() * 16) % 16 | 0;
                d = Math.floor(d / 16);
                return (c == 'x' ? r : (r & 0x3 | 0x8)).toString(16);
            });
            return uuid;
        },

    };


    window.__AE__ = {
      sessionStart : function () {
        tracker.sessionStart();
      },
      searchEvent : function (keyWord) {
          tracker.searchEvent(keyWord);
      },
      addCartEvent :function (gid) {
          tracker.addCartEvent(gid);
      }

    };

    window.__AE__.sessionStart();

})();








