package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName LoginEvent
 * @Description TODO
 * @Author ASUS
 * @Date 2021/7/20 15:10
 * @Version 1.0
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private Long userId;
    private String ip;
    private String eventType;
    private Long eventTime;
}
