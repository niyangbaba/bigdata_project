package com.dxh.bean

/**
  * Created by Administrator on 2018/12/27.
  */
class Top5Category(
                    var taskId: Int,
                    var category_id: Int,
                    var click_count: Int,
                    var cart_count: Int,
                    var order_count: Int,
                    var pay_count: Int
                  ) extends Ordered[Top5Category] with Serializable {


  override def compare(that: Top5Category): Int = {

    if (this.click_count - that.click_count != 0) {
      this.click_count - that.click_count
    } else if (this.cart_count - that.cart_count != 0) {
      this.cart_count - that.cart_count
    } else if (this.order_count - that.order_count != 0) {
      this.order_count - that.order_count
    } else {
      this.pay_count - that.pay_count
    }
  }

  override def toString = s"Top5Category(task_id=$taskId, category_id=$category_id, click_count=$click_count, cart_count=$cart_count, order_count=$order_count, pay_count=$pay_count)"
}
